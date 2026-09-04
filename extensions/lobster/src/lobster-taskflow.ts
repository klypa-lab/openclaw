// Lobster plugin module implements lobster taskflow behavior.
import crypto from "node:crypto";
import { asOptionalRecord, isRecord } from "openclaw/plugin-sdk/string-coerce-runtime";
import type { OpenClawPluginApi } from "../runtime-api.js";
import type { LobsterEnvelope, LobsterRunner, LobsterRunnerParams } from "./lobster-runner.js";

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

type LobsterTaskBinding = {
  schemaVersion: 1;
  ownerFlowId: string;
  taskId: string;
};

const TASK_FLOW_PROGRESS_EVENT_KEYS = new Set(["schemaVersion", "currentStep"]);
const MAX_TASK_FLOW_PROGRESS_EVENT_BYTES = 4096;
const MAX_TASK_FLOW_CURRENT_STEP_BYTES = 256;
const MAX_TERMINAL_SUMMARY_CHARS = 600;
const FAILED_TASK_SUMMARY = "Lobster workflow failed. Inspect /tasks for details before retrying.";
const STATUS_SAVE_FAILED_TASK_SUMMARY =
  "Lobster workflow status could not be saved. Inspect /tasks before retrying.";

function stateRecord(stateJson: JsonLike | undefined): Record<string, JsonLike> {
  if (stateJson && typeof stateJson === "object" && !Array.isArray(stateJson)) {
    return stateJson;
  }
  return stateJson === undefined ? {} : { workflowState: stateJson };
}

function attachLobsterTaskBinding(
  stateJson: JsonLike | undefined,
  binding: LobsterTaskBinding,
): JsonLike {
  return {
    ...stateRecord(stateJson),
    lobsterTask: binding,
  };
}

export function readLobsterTaskBinding(flow: FlowRecord): LobsterTaskBinding | undefined {
  const state = asOptionalRecord(flow.stateJson);
  const binding = asOptionalRecord(state?.lobsterTask);
  const taskId = typeof binding?.taskId === "string" ? binding.taskId.trim() : "";
  return binding?.schemaVersion === 1 && binding.ownerFlowId === flow.flowId && taskId
    ? { schemaVersion: 1, ownerFlowId: flow.flowId, taskId }
    : undefined;
}

function truncateSummary(value: string): string {
  const normalized = value.replace(/\s+/gu, " ").trim();
  return normalized.length <= MAX_TERMINAL_SUMMARY_CHARS
    ? normalized
    : `${normalized.slice(0, MAX_TERMINAL_SUMMARY_CHARS - 1).trimEnd()}…`;
}

function formatLobsterTaskSummary(envelope: Extract<LobsterEnvelope, { ok: true }>): string {
  if (envelope.status === "needs_approval") {
    const prompt = envelope.requiresApproval?.prompt.trim();
    return truncateSummary(prompt ? `Approval required: ${prompt}` : "Approval required.");
  }
  if (envelope.status === "cancelled") {
    return "Lobster workflow cancelled.";
  }
  if (envelope.output.length === 0) {
    return "Lobster workflow completed.";
  }
  let encoded: string;
  try {
    encoded = JSON.stringify(envelope.output);
  } catch {
    encoded = "Result is available in the workflow record.";
  }
  return truncateSummary(`Result: ${encoded}`);
}

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
  trackTask?: boolean;
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
  taskId?: string;
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
        if (params.taskId) {
          const taskProgress = params.taskFlow.recordTaskProgress({
            flowId: flow.flowId,
            taskId: params.taskId,
            progressSummary: flow.currentStep,
            eventSummary: flow.currentStep,
          });
          if (!taskProgress.applied) {
            throw new Error(`Task progress persistence failed: ${taskProgress.code}`);
          }
        }
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
    "taskFlow" | "config" | "runner" | "runnerParams" | "waitingStep"
  >,
  flow: FlowRecord,
  taskId?: string,
): Promise<ManagedLobsterFlowResult> {
  // Only revisions committed by this runner may advance its authority. Adopting
  // a refreshed revision could let a stale runner settle a replacement run.
  const ownedRevision: OwnedFlowRevision = { current: flow.revision };
  try {
    const envelope = await runWithTaskFlowCancellation({
      taskFlow: params.taskFlow,
      flowId: flow.flowId,
      ownedRevision,
      runner: params.runner,
      runnerParams: params.runnerParams,
      ...(taskId ? { taskId } : {}),
    });
    if (envelope.ok && envelope.status === "cancelled") {
      try {
        const requested = params.taskFlow.requestCancel({
          flowId: flow.flowId,
          expectedRevision: ownedRevision.current,
        });
        if (!requested.applied) {
          return {
            ok: false,
            flow: asManagedFlow(params.taskFlow.get(flow.flowId)) ?? flow,
            mutation: requested,
            error: new Error(`TaskFlow cancellation failed: ${requested.code}`),
          };
        }
        ownedRevision.current = requested.flow.revision;
        const mutation = await params.taskFlow.cancel({
          flowId: flow.flowId,
          cfg: params.config,
        });
        const current = asManagedFlow(params.taskFlow.get(flow.flowId)) ?? flow;
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
          flow: asManagedFlow(params.taskFlow.get(flow.flowId)) ?? flow,
          error: err,
        };
      }
    }
    if (!envelope.ok) {
      const mutation = params.taskFlow.fail({
        flowId: flow.flowId,
        expectedRevision: ownedRevision.current,
      });
      if (taskId) {
        params.taskFlow.finalizeTask({
          flowId: flow.flowId,
          taskId,
          status: "failed",
          error: envelope.error.message,
          terminalSummary: FAILED_TASK_SUMMARY,
        });
      }
      return {
        ok: false,
        flow: mutation.applied
          ? mutation.flow
          : (asManagedFlow(params.taskFlow.get(flow.flowId)) ?? flow),
        mutation,
        error: new Error(envelope.error.message),
      };
    }
    const mutation =
      envelope.status === "needs_approval"
        ? params.taskFlow.setWaiting({
            flowId: flow.flowId,
            expectedRevision: ownedRevision.current,
            currentStep: params.waitingStep ?? "await_lobster_approval",
            waitJson: buildApprovalWaitState(envelope),
          })
        : params.taskFlow.finish({
            flowId: flow.flowId,
            expectedRevision: ownedRevision.current,
          });
    if (!mutation.applied) {
      if (taskId) {
        params.taskFlow.finalizeTask({
          flowId: flow.flowId,
          taskId,
          status: "failed",
          error: `TaskFlow transition failed: ${mutation.code}`,
          terminalSummary: STATUS_SAVE_FAILED_TASK_SUMMARY,
        });
      }
      return {
        ok: false,
        flow: asManagedFlow(params.taskFlow.get(flow.flowId)) ?? flow,
        mutation,
        error: new Error(`TaskFlow transition failed: ${mutation.code}`),
      };
    }
    if (taskId) {
      const taskMutation =
        envelope.status === "needs_approval"
          ? params.taskFlow.recordTaskProgress({
              flowId: flow.flowId,
              taskId,
              progressSummary: formatLobsterTaskSummary(envelope),
              eventSummary: "Approval required",
            })
          : params.taskFlow.finalizeTask({
              flowId: flow.flowId,
              taskId,
              status: "succeeded",
              terminalSummary: formatLobsterTaskSummary(envelope),
            });
      if (!taskMutation.applied) {
        return {
          ok: false,
          flow: mutation.flow,
          mutation,
          error: new Error(`Task status update failed: ${taskMutation.code}`),
        };
      }
    }
    return { ok: true, envelope, flow: mutation.flow, mutation };
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error));
    const persisted = asManagedFlow(params.taskFlow.get(flow.flowId));
    // Cancellation is already terminal state; a runner rejection must not
    // replace it with failure after the abort reaches embedded Lobster work.
    if (persisted?.status === "cancelled" || persisted?.cancelRequestedAt != null) {
      if (taskId) {
        params.taskFlow.finalizeTask({
          flowId: flow.flowId,
          taskId,
          status: "cancelled",
          terminalSummary: "Lobster workflow cancelled.",
        });
      }
      return { ok: false, flow: persisted, error: err };
    }
    try {
      const mutation = params.taskFlow.fail({
        flowId: flow.flowId,
        expectedRevision: ownedRevision.current,
      });
      if (taskId) {
        params.taskFlow.finalizeTask({
          flowId: flow.flowId,
          taskId,
          status: "failed",
          error: err.message,
          terminalSummary: FAILED_TASK_SUMMARY,
        });
      }
      return {
        ok: false,
        flow: mutation.applied
          ? mutation.flow
          : (asManagedFlow(params.taskFlow.get(flow.flowId)) ?? flow),
        mutation,
        error: err,
      };
    } catch {
      return {
        ok: false,
        flow: asManagedFlow(params.taskFlow.get(flow.flowId)) ?? flow,
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
  let activeFlow = started.flow;
  let taskId: string | undefined;
  if (params.trackTask) {
    const task = params.taskFlow.runTask({
      flowId: activeFlow.flowId,
      runtime: "cli",
      label: params.goal,
      task: params.goal,
      status: "running",
      notifyPolicy: "done_only",
      progressSummary: activeFlow.currentStep ?? "Running Lobster workflow",
    });
    if (!task.created) {
      const failed = params.taskFlow.fail({
        flowId: activeFlow.flowId,
        expectedRevision: activeFlow.revision,
        blockedSummary: task.reason,
      });
      return {
        ok: false,
        flow: failed.applied ? failed.flow : activeFlow,
        mutation: failed,
        error: new Error(`Task registration failed: ${task.reason}`),
      };
    }
    taskId = task.task.taskId;
    const binding = attachLobsterTaskBinding(activeFlow.stateJson, {
      schemaVersion: 1,
      ownerFlowId: activeFlow.flowId,
      taskId,
    });
    const bound = params.taskFlow.updateProgress({
      flowId: activeFlow.flowId,
      expectedRevision: activeFlow.revision,
      currentStep: activeFlow.currentStep,
      stateJson: binding,
    });
    if (!bound.applied) {
      params.taskFlow.finalizeTask({
        flowId: activeFlow.flowId,
        taskId,
        status: "failed",
        error: `TaskFlow task binding failed: ${bound.code}`,
        terminalSummary: "Lobster workflow could not start.",
      });
      const failed = params.taskFlow.fail({
        flowId: activeFlow.flowId,
        expectedRevision: activeFlow.revision,
        blockedSummary: `TaskFlow task binding failed: ${bound.code}`,
      });
      return {
        ok: false,
        flow: failed.applied ? failed.flow : activeFlow,
        mutation: failed,
        error: new Error(`TaskFlow task binding failed: ${bound.code}`),
      };
    }
    activeFlow = bound.flow;
  }
  const completion = executeManagedLobsterFlow(params, activeFlow, taskId);
  return { ok: true, created: true, flow: activeFlow, completion };
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
  const binding = readLobsterTaskBinding(resumed.flow);
  return await executeManagedLobsterFlow(params, resumed.flow, binding?.taskId);
}
