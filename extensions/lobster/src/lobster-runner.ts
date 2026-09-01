// Lobster plugin module implements lobster runner behavior.
import { stat } from "node:fs/promises";
import path from "node:path";
import { Readable, Writable } from "node:stream";
import { toErrorObject as toLintErrorObject } from "openclaw/plugin-sdk/error-runtime";
import { isPathInside } from "openclaw/plugin-sdk/file-access-runtime";

export type LobsterEnvelope =
  | {
      ok: true;
      status: "ok" | "needs_approval" | "cancelled";
      output: unknown[];
      requiresApproval: null | {
        type: "approval_request";
        prompt: string;
        items: unknown[];
        resumeToken?: string;
        approvalId?: string;
      };
    }
  | {
      ok: false;
      error: { type?: string; message: string };
    };

export type LobsterRunnerParams = {
  action: "run" | "resume";
  pipeline?: string;
  argsJson?: string;
  token?: string;
  approvalId?: string;
  approve?: boolean;
  cwd: string;
  timeoutMs: number;
  maxStdoutBytes: number;
  signal?: AbortSignal;
};

export type LobsterRunner = {
  run: (params: LobsterRunnerParams) => Promise<LobsterEnvelope>;
};

type EmbeddedToolContext = {
  cwd?: string;
  env?: Record<string, string | undefined>;
  mode?: "tool" | "human" | "sdk";
  stdin?: NodeJS.ReadableStream;
  stdout?: NodeJS.WritableStream;
  stderr?: NodeJS.WritableStream;
  signal?: AbortSignal;
};

type EmbeddedToolEnvelope = {
  ok: boolean;
  status?: "ok" | "needs_approval" | "needs_input" | "cancelled";
  output?: unknown[];
  requiresApproval?: {
    prompt: string;
    items: unknown[];
    resumeToken?: string;
    approvalId?: string;
  } | null;
  error?: {
    message: string;
  };
};

type EmbeddedToolRuntime = {
  runToolRequest: (params: {
    pipeline?: string;
    filePath?: string;
    args?: Record<string, unknown>;
    ctx?: EmbeddedToolContext;
  }) => Promise<EmbeddedToolEnvelope>;
  resumeToolRequest: (params: {
    token?: string;
    approvalId?: string;
    approved?: boolean;
    ctx?: EmbeddedToolContext;
  }) => Promise<EmbeddedToolEnvelope>;
};

const workflowExts = new Set([".lobster", ".yaml", ".yml", ".json"]);

export function resolveLobsterCwd(cwdRaw: unknown): string {
  if (typeof cwdRaw !== "string" || !cwdRaw.trim()) {
    return process.cwd();
  }
  const cwd = cwdRaw.trim();
  if (path.isAbsolute(cwd)) {
    throw new Error("cwd must be a relative path");
  }
  const base = process.cwd();
  const resolved = path.resolve(base, cwd);

  if (!isPathInside(base, resolved)) {
    throw new Error("cwd must stay within the gateway working directory");
  }
  return resolved;
}

function createLimitedSink(maxBytes: number, label: "stdout" | "stderr") {
  let bytes = 0;
  return new Writable({
    write(chunk, _encoding, callback) {
      bytes += Buffer.byteLength(String(chunk), "utf8");
      if (bytes > maxBytes) {
        callback(new Error(`lobster ${label} exceeded maxStdoutBytes`));
        return;
      }
      callback();
    },
  });
}

function normalizeEnvelope(
  envelope: EmbeddedToolEnvelope,
  maxStdoutBytes: number,
): Extract<LobsterEnvelope, { ok: true }> {
  if (!envelope.ok) {
    throw new Error(envelope.error?.message ?? "lobster runtime failed");
  }
  if (envelope.status === "needs_input") {
    throw new Error("Lobster input requests are not supported by the OpenClaw Lobster tool yet");
  }
  const normalized: Extract<LobsterEnvelope, { ok: true }> = {
    ok: true,
    status: envelope.status ?? "ok",
    output: Array.isArray(envelope.output) ? envelope.output : [],
    requiresApproval: envelope.requiresApproval
      ? {
          type: "approval_request",
          prompt: envelope.requiresApproval.prompt,
          items: envelope.requiresApproval.items,
          ...(envelope.requiresApproval.resumeToken
            ? { resumeToken: envelope.requiresApproval.resumeToken }
            : {}),
          ...(envelope.requiresApproval.approvalId
            ? { approvalId: envelope.requiresApproval.approvalId }
            : {}),
        }
      : null,
  };
  if (Buffer.byteLength(JSON.stringify(normalized, null, 2), "utf8") > maxStdoutBytes) {
    throw new Error("lobster runtime result exceeded maxStdoutBytes");
  }
  return normalized;
}

function isMissingPathError(error: unknown) {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: unknown }).code === "ENOENT"
  );
}

async function detectWorkflowFile(candidate: string, cwd: string) {
  const trimmed = candidate.trim();
  if (!trimmed || trimmed.includes("|") || !workflowExts.has(path.extname(trimmed).toLowerCase())) {
    return null;
  }
  const resolved = path.isAbsolute(trimmed) ? trimmed : path.resolve(cwd, trimmed);
  try {
    if (!(await stat(resolved)).isFile()) {
      throw new Error("Workflow path is not a file");
    }
    return resolved;
  } catch (error) {
    if (/\s/.test(trimmed) && isMissingPathError(error)) {
      return null;
    }
    throw error;
  }
}

function createEmbeddedToolContext(
  params: LobsterRunnerParams,
  signal?: AbortSignal,
): EmbeddedToolContext {
  const env = { ...process.env } as Record<string, string | undefined>;
  return {
    cwd: params.cwd,
    env,
    mode: "tool",
    stdin: Readable.from([]),
    stdout: createLimitedSink(Math.max(1024, params.maxStdoutBytes), "stdout"),
    stderr: createLimitedSink(Math.max(1024, params.maxStdoutBytes), "stderr"),
    signal,
  };
}

async function withTimeout<T>(
  timeoutMs: number,
  externalSignal: AbortSignal | undefined,
  fn: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const timeout = Math.max(200, timeoutMs);
  const timeoutController = new AbortController();
  const signal = externalSignal
    ? AbortSignal.any([externalSignal, timeoutController.signal])
    : timeoutController.signal;
  let onAbort = () => {};
  const timer = setTimeout(
    () => timeoutController.abort(new Error("lobster runtime timed out")),
    timeout,
  );
  try {
    return await new Promise<T>((resolve, reject) => {
      onAbort = () =>
        reject(
          toLintErrorObject(
            signal.reason ?? new DOMException("The operation was aborted", "AbortError"),
            "Non-Error abort reason",
          ),
        );
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) {
        onAbort();
        return;
      }
      void fn(signal).then(resolve, (error: unknown) => {
        reject(toLintErrorObject(error, "Non-Error rejection"));
      });
    });
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", onAbort);
  }
}

async function loadEmbeddedToolRuntimeFromPackage(): Promise<EmbeddedToolRuntime> {
  // Joined specifier keeps bundlers from statically resolving
  // @clawdbot/lobster/core; the plugin's declared @clawdbot/lobster dependency
  // provides it at runtime, so it is a used direct dependency.
  const coreSpecifier = ["@clawdbot", "lobster", "core"].join("/");
  return (await import(coreSpecifier)) as EmbeddedToolRuntime;
}

export function createEmbeddedLobsterRunner(options?: {
  loadRuntime?: () => Promise<EmbeddedToolRuntime>;
}): LobsterRunner {
  const loadRuntime = options?.loadRuntime ?? loadEmbeddedToolRuntimeFromPackage;
  let runtimePromise: Promise<EmbeddedToolRuntime> | undefined;
  return {
    async run(params) {
      return await withTimeout(params.timeoutMs, params.signal, async (signal) => {
        runtimePromise ??= loadRuntime();
        const runtime = await runtimePromise;
        signal.throwIfAborted();
        const ctx = createEmbeddedToolContext(params, signal);
        let envelope: EmbeddedToolEnvelope;

        if (params.action === "run") {
          const pipeline = params.pipeline?.trim() ?? "";
          if (!pipeline) {
            throw new Error("pipeline required");
          }

          const filePath = await detectWorkflowFile(pipeline, params.cwd);
          if (filePath) {
            const parsedArgsJson = params.argsJson?.trim() ?? "";
            let args: Record<string, unknown> | undefined;
            if (parsedArgsJson) {
              try {
                args = JSON.parse(parsedArgsJson) as Record<string, unknown>;
              } catch {
                throw new Error("run --args-json must be valid JSON");
              }
            }
            envelope = await runtime.runToolRequest({ filePath, args, ctx });
          } else {
            envelope = await runtime.runToolRequest({ pipeline, ctx });
          }
        } else {
          const token = params.token?.trim() ?? "";
          const approvalId = params.approvalId?.trim() ?? "";
          if (!token && !approvalId) {
            throw new Error("token or approvalId required");
          }
          if (typeof params.approve !== "boolean") {
            throw new Error("approve required");
          }
          envelope = await runtime.resumeToolRequest({
            ...(token ? { token } : {}),
            ...(approvalId ? { approvalId } : {}),
            approved: params.approve,
            ctx,
          });
        }
        return normalizeEnvelope(envelope, Math.max(1024, params.maxStdoutBytes));
      });
    },
  };
}
