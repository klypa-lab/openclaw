// Lobster plugin module implements lobster taskflow behavior.
import crypto from "node:crypto";
import { isRecord } from "openclaw/plugin-sdk/string-coerce-runtime";
import type { OpenClawPluginApi } from "../runtime-api.js";
import type { LobsterEnvelope, LobsterRunner, LobsterRunnerParams } from "./lobster-runner.js";
import { attachManagedStatusMessage, type ManagedStatusMessageReceipt } from "./status-message.js";

export type JsonLike =
  | null
  | boolean
  | number
  | string
  | JsonLike[]
  | {
      [key: string]: JsonLike;
    };

export type BoundTaskFlow = ReturnType<
  NonNullable<OpenClawPluginApi["runtime"]>["tasks"]["managedFlows"]["bindSession"]
>;

type FlowRecord = NonNullable<ReturnType<BoundTaskFlow["tryCreateManaged"]>>;

export type ManagedLobsterFlowUpdate = (flow: FlowRecord) => Promise<void>;

export const LOBSTER_RUNNER_LEASE = Object.freeze({
  ownerId: "lobster",
  leaseId: crypto.randomUUID(),
});
type FlowMutationResult = ReturnType<BoundTaskFlow["finish"]>;
type MutationResult = FlowMutationResult | Awaited<ReturnType<BoundTaskFlow["cancel"]>>;
type OwnedFlowRevision = { current: number };

type LobsterApprovalWaitState = {
  kind: "lobster_approval";
  prompt: string;
  items: JsonLike[];
  resumeToken?: string;
  approvalId?: string;
};

type LobsterTaskFlowProgressEvent = {
  schemaVersion: 1;
  currentStep: string;
};

const TASK_FLOW_PROGRESS_EVENT_KEYS = new Set(["schemaVersion", "currentStep"]);
const MAX_TASK_FLOW_PROGRESS_EVENT_BYTES = 4096;
const MAX_TASK_FLOW_CURRENT_STEP_BYTES = 256;

type RunManagedLobsterFlowParams = {
  taskFlow: BoundTaskFlow;
  config: OpenClawPluginApi["config"];
  runner: LobsterRunner;
  runnerParams: LobsterRunnerParams;
  controllerId: string;
  goal: string;
  stateJson?: JsonLike;
  currentStep?: string;
  waitingStep?: string;
  invocation: { runId: string; toolCallId: string };
  createStatusMessage?: (flow: FlowRecord) => Promise<ManagedStatusMessageReceipt | undefined>;
  onFlowUpdate?: ManagedLobsterFlowUpdate;
};

type ResumeManagedLobsterFlowParams = {
  taskFlow: BoundTaskFlow;
  config: OpenClawPluginApi["config"];
  runner: LobsterRunner;
  runnerParams: LobsterRunnerParams & {
    action: "resume";
    approve: boolean;
  } & ({ token: string } | { approvalId: string });
  flowId: string;
  expectedRevision: number;
  currentStep?: string;
  waitingStep?: string;
  onFlowUpdate?: ManagedLobsterFlowUpdate;
};

export type ManagedLobsterFlowResult =
  | {
      ok: true;
      envelope: LobsterEnvelope;
      flow: FlowRecord;
      mutation: MutationResult;
    }
  | {
      ok: true;
      replayed: true;
      flow: FlowRecord;
    }
  | {
      ok: false;
      flow?: FlowRecord;
      mutation?: MutationResult;
      error: Error;
    };

export type ManagedLobsterFlowLaunchResult =
  | {
      ok: true;
      created: true;
      flow: FlowRecord;
      completion: Promise<ManagedLobsterFlowResult>;
    }
  | {
      ok: true;
      created: false;
      flow: FlowRecord;
    }
  | Extract<ManagedLobsterFlowResult, { ok: false }>;

function toJsonLike(value: unknown, seen = new WeakSet<object>()): JsonLike {
  if (value === null) {
    return null;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : String(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "boolean" || typeof value === "string") {
    return value;
  }
  if (typeof value !== "object") {
    return null;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (seen.has(value)) {
    return "[Circular]";
  }
  seen.add(value);
  if (Array.isArray(value)) {
    const jsonArray = value.map((item) => toJsonLike(item, seen));
    seen.delete(value);
    return jsonArray;
  }
  const jsonObject: Record<string, JsonLike> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (entry === undefined || typeof entry === "function" || typeof entry === "symbol") {
      continue;
    }
    jsonObject[key] = toJsonLike(entry, seen);
  }
  seen.delete(value);
  return jsonObject;
}

function buildApprovalWaitState(envelope: Extract<LobsterEnvelope, { ok: true }>): JsonLike {
  const approval = envelope.requiresApproval;
  return {
    kind: "lobster_approval",
    prompt: approval ? approval.prompt : "",
    items: approval ? approval.items.map((item) => toJsonLike(item)) : [],
    ...(approval?.resumeToken ? { resumeToken: approval.resumeToken } : {}),
    ...(approval?.approvalId ? { approvalId: approval.approvalId } : {}),
  } satisfies LobsterApprovalWaitState;
}

function parseTaskFlowProgressEvent(event: unknown): LobsterTaskFlowProgressEvent {
  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(event);
  } catch {
    throw new Error("TaskFlow progress event must be JSON serializable");
  }
  if (encoded === undefined) {
    throw new Error("TaskFlow progress event must be JSON serializable");
  }
  if (Buffer.byteLength(encoded, "utf8") > MAX_TASK_FLOW_PROGRESS_EVENT_BYTES) {
    throw new Error("TaskFlow progress event exceeds 4 KiB");
  }
  if (!isRecord(event)) {
    throw new Error("TaskFlow progress event must be a JSON object");
  }
  const unknownKey = Object.keys(event).find((key) => !TASK_FLOW_PROGRESS_EVENT_KEYS.has(key));
  if (unknownKey) {
    throw new Error(`TaskFlow progress event contains unknown field: ${unknownKey}`);
  }
  if (event.schemaVersion !== 1) {
    throw new Error("TaskFlow progress event requires schemaVersion 1");
  }
  if (typeof event.currentStep !== "string" || !event.currentStep.trim()) {
    throw new Error("TaskFlow progress event requires currentStep");
  }
  const currentStep = event.currentStep.trim();
  if (Buffer.byteLength(currentStep, "utf8") > MAX_TASK_FLOW_CURRENT_STEP_BYTES) {
    throw new Error("TaskFlow progress event currentStep exceeds 256 bytes");
  }
  return { schemaVersion: 1, currentStep };
}

function isManagedFlow(value: ReturnType<BoundTaskFlow["get"]>): value is FlowRecord {
  return value?.syncMode === "managed" && typeof value.controllerId === "string";
}

function asManagedFlow(value: ReturnType<BoundTaskFlow["get"]>): FlowRecord | undefined {
  return isManagedFlow(value) ? value : undefined;
}

async function publishFlowUpdate(
  onFlowUpdate: ManagedLobsterFlowUpdate | undefined,
  flow: FlowRecord,
): Promise<void> {
  if (!onFlowUpdate) {
    return;
  }
  try {
    await onFlowUpdate(flow);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[lobster] status message update failed flow=${flow.flowId} revision=${String(flow.revision)}: ${message}`,
    );
  }
}

function applyManagedLobsterProgress(params: {
  taskFlow: BoundTaskFlow;
  flowId: string;
  expectedRevision: number;
  event: unknown;
}): FlowRecord {
  const event = parseTaskFlowProgressEvent(params.event);
  const mutation = params.taskFlow.updateProgress({
    flowId: params.flowId,
    expectedRevision: params.expectedRevision,
    currentStep: event.currentStep,
  });
  if (mutation.applied) {
    return mutation.flow;
  }
  throw new Error(`TaskFlow progress event persistence failed: ${mutation.code}`);
}

async function runWithTaskFlowCancellation(params: {
  taskFlow: BoundTaskFlow;
  flowId: string;
  ownedRevision: OwnedFlowRevision;
  runner: LobsterRunner;
  runnerParams: LobsterRunnerParams;
  onFlowUpdate?: ManagedLobsterFlowUpdate;
}): Promise<LobsterEnvelope> {
  const controller = new AbortController();
  const checkCancellation = () => {
    const flow = params.taskFlow.get(params.flowId);
    if (
      (flow?.status === "cancelled" || flow?.cancelRequestedAt != null) &&
      !controller.signal.aborted
    ) {
      controller.abort(new Error("TaskFlow cancellation requested"));
    }
  };
  // The TaskFlow record is authoritative. Check before admission, then keep the
  // embedded runtime tied to cancellation for the full execution lifetime.
  checkCancellation();
  if (controller.signal.aborted) {
    throw controller.signal.reason;
  }
  const timer = setInterval(checkCancellation, 100);
  timer.unref?.();
  try {
    const signal = params.runnerParams.signal
      ? AbortSignal.any([params.runnerParams.signal, controller.signal])
      : controller.signal;
    const envelope = await params.runner.run({
      ...params.runnerParams,
      signal,
      onTaskFlowEvent: async (event) => {
        const flow = applyManagedLobsterProgress({
          taskFlow: params.taskFlow,
          flowId: params.flowId,
          expectedRevision: params.ownedRevision.current,
          event,
        });
        params.ownedRevision.current = flow.revision;
        await publishFlowUpdate(params.onFlowUpdate, flow);
      },
    });
    checkCancellation();
    controller.signal.throwIfAborted();
    return envelope;
  } finally {
    clearInterval(timer);
  }
}

async function executeManagedLobsterFlow(
  params: Pick<
    RunManagedLobsterFlowParams,
    | "taskFlow"
    | "config"
    | "runner"
    | "runnerParams"
    | "waitingStep"
    | "createStatusMessage"
    | "onFlowUpdate"
  >,
  flow: FlowRecord,
): Promise<ManagedLobsterFlowResult> {
  // Only revisions committed by this runner may advance its authority. Adopting
  // a refreshed revision could let a stale runner settle a replacement run.
  const ownedRevision: OwnedFlowRevision = { current: flow.revision };
  let activeFlow = flow;
  let statusStateForFailure: JsonLike | undefined;
  try {
    if (params.createStatusMessage) {
      const receipt = await params.createStatusMessage(flow);
      if (receipt) {
        statusStateForFailure = attachManagedStatusMessage(flow.stateJson, flow.flowId, receipt);
        const bound = params.taskFlow.updateProgress({
          flowId: flow.flowId,
          expectedRevision: ownedRevision.current,
          currentStep: flow.currentStep,
          stateJson: statusStateForFailure,
        });
        if (!bound.applied) {
          throw new Error(`TaskFlow status message bind failed: ${bound.code}`);
        }
        activeFlow = bound.flow;
        ownedRevision.current = bound.flow.revision;
        statusStateForFailure = undefined;
      }
    }
    const envelope = await runWithTaskFlowCancellation({
      taskFlow: params.taskFlow,
      flowId: activeFlow.flowId,
      ownedRevision,
      runner: params.runner,
      runnerParams: params.runnerParams,
      ...(params.onFlowUpdate ? { onFlowUpdate: params.onFlowUpdate } : {}),
    });
    if (envelope.ok && envelope.status === "cancelled") {
      try {
        const requested = params.taskFlow.requestCancel({
          flowId: activeFlow.flowId,
          expectedRevision: ownedRevision.current,
        });
        if (!requested.applied) {
          return {
            ok: false,
            flow: asManagedFlow(params.taskFlow.get(activeFlow.flowId)) ?? activeFlow,
            mutation: requested,
            error: new Error(`TaskFlow cancellation failed: ${requested.code}`),
          };
        }
        ownedRevision.current = requested.flow.revision;
        const mutation = await params.taskFlow.cancel({
          flowId: activeFlow.flowId,
          cfg: params.config,
        });
        const current = asManagedFlow(params.taskFlow.get(activeFlow.flowId)) ?? activeFlow;
        await publishFlowUpdate(params.onFlowUpdate, current);
        return mutation.cancelled
          ? { ok: true, envelope, flow: current, mutation }
          : {
              ok: false,
              flow: current,
              mutation,
              error: new Error(`TaskFlow cancellation failed: ${mutation.reason ?? "unknown"}`),
            };
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        return {
          ok: false,
          flow: asManagedFlow(params.taskFlow.get(activeFlow.flowId)) ?? activeFlow,
          error: err,
        };
      }
    }
    if (!envelope.ok) {
      const mutation = params.taskFlow.fail({
        flowId: activeFlow.flowId,
        expectedRevision: ownedRevision.current,
      });
      if (mutation.applied) {
        await publishFlowUpdate(params.onFlowUpdate, mutation.flow);
      }
      return {
        ok: false,
        flow: mutation.applied
          ? mutation.flow
          : (asManagedFlow(params.taskFlow.get(activeFlow.flowId)) ?? activeFlow),
        mutation,
        error: new Error(envelope.error.message),
      };
    }
    const mutation =
      envelope.status === "needs_approval"
        ? params.taskFlow.setWaiting({
            flowId: activeFlow.flowId,
            expectedRevision: ownedRevision.current,
            currentStep: params.waitingStep ?? "await_lobster_approval",
            waitJson: buildApprovalWaitState(envelope),
          })
        : params.taskFlow.finish({
            flowId: activeFlow.flowId,
            expectedRevision: ownedRevision.current,
          });
    if (!mutation.applied) {
      return {
        ok: false,
        flow: asManagedFlow(params.taskFlow.get(activeFlow.flowId)) ?? activeFlow,
        mutation,
        error: new Error(`TaskFlow transition failed: ${mutation.code}`),
      };
    }
    await publishFlowUpdate(params.onFlowUpdate, mutation.flow);
    return { ok: true, envelope, flow: mutation.flow, mutation };
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error));
    const persisted = asManagedFlow(params.taskFlow.get(activeFlow.flowId));
    // Cancellation is already terminal state; a runner rejection must not
    // replace it with failure after the abort reaches embedded Lobster work.
    if (persisted?.status === "cancelled" || persisted?.cancelRequestedAt != null) {
      return { ok: false, flow: persisted, error: err };
    }
    try {
      const mutation = params.taskFlow.fail({
        flowId: activeFlow.flowId,
        expectedRevision: ownedRevision.current,
        ...(statusStateForFailure !== undefined ? { stateJson: statusStateForFailure } : {}),
      });
      if (mutation.applied) {
        await publishFlowUpdate(params.onFlowUpdate, mutation.flow);
      }
      return {
        ok: false,
        flow: mutation.applied
          ? mutation.flow
          : (asManagedFlow(params.taskFlow.get(activeFlow.flowId)) ?? activeFlow),
        mutation,
        error: err,
      };
    } catch {
      return {
        ok: false,
        flow: asManagedFlow(params.taskFlow.get(activeFlow.flowId)) ?? activeFlow,
        error: err,
      };
    }
  }
}

export function launchManagedLobsterFlow(
  params: RunManagedLobsterFlowParams,
): ManagedLobsterFlowLaunchResult {
  const started = params.taskFlow.startManaged({
    controllerId: params.controllerId,
    goal: params.goal,
    runId: params.invocation.runId,
    toolCallId: params.invocation.toolCallId,
    currentStep: params.currentStep ?? "run_lobster",
    runnerLease: LOBSTER_RUNNER_LEASE,
    requestJson: {
      runnerParams: toJsonLike(params.runnerParams),
      waitingStep: params.waitingStep ?? null,
    },
    ...(params.stateJson !== undefined ? { stateJson: params.stateJson } : {}),
  });
  if (!started.ok) {
    return {
      ok: false,
      error: new Error(`TaskFlow start failed: ${started.code}`),
    };
  }
  if (!started.created) {
    return { ok: true, created: false, flow: started.flow };
  }
  const completion = executeManagedLobsterFlow(params, started.flow);
  return { ok: true, created: true, flow: started.flow, completion };
}

export async function resumeManagedLobsterFlow(
  params: ResumeManagedLobsterFlowParams,
): Promise<ManagedLobsterFlowResult> {
  const resumed = params.taskFlow.resume({
    flowId: params.flowId,
    expectedRevision: params.expectedRevision,
    status: "running",
    currentStep: params.currentStep ?? "resume_lobster",
    runnerLease: LOBSTER_RUNNER_LEASE,
  });

  if (!resumed.applied) {
    return {
      ok: false,
      mutation: resumed,
      error: new Error(`TaskFlow resume failed: ${resumed.code}`),
    };
  }
  await publishFlowUpdate(params.onFlowUpdate, resumed.flow);
  return await executeManagedLobsterFlow(params, resumed.flow);
}
