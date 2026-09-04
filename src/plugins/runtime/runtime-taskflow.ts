// Runtime task-flow helpers adapt plugin task descriptors into executable task flows.
import { isDeepStrictEqual } from "node:util";
import { getTaskById, updateTaskStateById } from "../../tasks/runtime-internal.js";
import { isTerminalTaskStatus } from "../../tasks/task-executor-policy.js";
import {
  cancelFlowByIdForOwner,
  getFlowTaskSummary,
  runTaskInFlowForOwner,
} from "../../tasks/task-executor.js";
import {
  findLatestTaskFlowForOwner,
  getTaskFlowByIdForOwner,
  listTaskFlowsForOwner,
  resolveTaskFlowForLookupTokenForOwner,
} from "../../tasks/task-flow-owner-access.js";
import {
  isTerminalTaskFlow,
  type JsonValue,
  type TaskFlowRecord,
} from "../../tasks/task-flow-registry.types.js";
import {
  claimManagedTaskFlowStart,
  createManagedTaskFlow,
  failFlow,
  finishFlow,
  listTaskFlowRecords,
  type TaskFlowUpdateResult,
  requestFlowCancel,
  resumeFlow,
  setFlowWaiting,
  updateFlowProgress,
} from "../../tasks/task-flow-runtime-internal.js";
import type { TaskDeliveryState, TaskRecord } from "../../tasks/task-registry.types.js";
import { normalizeDeliveryContext } from "../../utils/delivery-context.shared.js";
import type {
  BoundTaskFlowRuntime,
  ManagedTaskFlowMutationResult,
  ManagedTaskFlowRecord,
  PluginRuntimeTaskFlow,
} from "./runtime-taskflow.types.js";

function assertSessionKey(sessionKey: string | undefined, errorMessage: string): string {
  const normalized = sessionKey?.trim();
  if (!normalized) {
    throw new Error(errorMessage);
  }
  return normalized;
}

function asManagedTaskFlowRecord(
  flow: TaskFlowRecord | undefined,
): ManagedTaskFlowRecord | undefined {
  if (!flow || flow.syncMode !== "managed" || !flow.controllerId) {
    return undefined;
  }
  return flow as ManagedTaskFlowRecord;
}

function mapFlowUpdateResult(result: TaskFlowUpdateResult): ManagedTaskFlowMutationResult {
  if (result.applied) {
    const managed = asManagedTaskFlowRecord(result.flow);
    if (!managed) {
      return {
        applied: false,
        code: "not_managed",
        current: result.flow,
      };
    }
    return {
      applied: true,
      flow: managed,
    };
  }
  return {
    applied: false,
    code: result.reason,
    ...(result.current ? { current: result.current } : {}),
  };
}

function applyManagedFlowMutationForOwner(params: {
  flowId: string;
  ownerKey: string;
  mutate: (flowId: string) => TaskFlowUpdateResult;
  replay?: (flow: ManagedTaskFlowRecord) => boolean;
}): ManagedTaskFlowMutationResult {
  // Authorization and mode checks must complete before the mutation can touch persistence.
  const flow = getTaskFlowByIdForOwner({
    flowId: params.flowId,
    callerOwnerKey: params.ownerKey,
  });
  if (!flow) {
    return { applied: false, code: "not_found" };
  }
  const managed = asManagedTaskFlowRecord(flow);
  if (!managed) {
    return { applied: false, code: "not_managed", current: flow };
  }
  // A controller may retry a completion after losing its first response. Preserve the
  // terminal result and only acknowledge a byte-equivalent semantic replay.
  if (isTerminalTaskFlow(managed)) {
    return params.replay?.(managed)
      ? { applied: true, flow: managed }
      : { applied: false, code: "revision_conflict", current: managed };
  }
  return mapFlowUpdateResult(params.mutate(managed.flowId));
}

function matchesTerminalReplay(params: {
  flow: ManagedTaskFlowRecord;
  expectedRevision: number;
  status: "succeeded" | "failed";
  stateJson?: JsonValue | null;
  blockedTaskId?: string | null;
  blockedSummary?: string | null;
  updatedAt?: number;
  endedAt?: number;
}): boolean {
  const { flow } = params;
  if (flow.revision !== params.expectedRevision + 1 || flow.status !== params.status) {
    return false;
  }
  const matchesOptionalString = (
    current: string | undefined,
    requested: string | null | undefined,
  ) => requested === undefined || current === (requested?.trim() || undefined);
  return (
    (params.stateJson === undefined || isDeepStrictEqual(flow.stateJson, params.stateJson)) &&
    matchesOptionalString(flow.blockedTaskId, params.blockedTaskId) &&
    matchesOptionalString(flow.blockedSummary, params.blockedSummary) &&
    (params.updatedAt === undefined || flow.updatedAt === params.updatedAt) &&
    (params.endedAt === undefined || flow.endedAt === params.endedAt)
  );
}

function resolveManagedFlowTask(params: {
  ownerKey: string;
  flowId: string;
  taskId: string;
}): TaskRecord | undefined {
  const flow = getTaskFlowByIdForOwner({
    flowId: params.flowId,
    callerOwnerKey: params.ownerKey,
  });
  if (!flow || flow.syncMode !== "managed") {
    return undefined;
  }
  const task = getTaskById(params.taskId);
  return task?.scopeKind === "session" &&
    task.ownerKey.trim() === params.ownerKey &&
    task.parentFlowId?.trim() === flow.flowId
    ? task
    : undefined;
}

function createBoundTaskFlowRuntime(params: {
  sessionKey: string;
  requesterOrigin?: TaskDeliveryState["requesterOrigin"];
}): BoundTaskFlowRuntime {
  const ownerKey = assertSessionKey(
    params.sessionKey,
    "TaskFlow runtime requires a bound sessionKey.",
  );
  const requesterOrigin = params.requesterOrigin
    ? normalizeDeliveryContext(params.requesterOrigin)
    : undefined;
  const tryCreateManaged: BoundTaskFlowRuntime["tryCreateManaged"] = (input) => {
    const flow = createManagedTaskFlow({
      ownerKey,
      controllerId: input.controllerId,
      requesterOrigin,
      status: input.status,
      notifyPolicy: input.notifyPolicy,
      goal: input.goal,
      currentStep: input.currentStep,
      stateJson: input.stateJson,
      waitJson: input.waitJson,
      cancelRequestedAt: input.cancelRequestedAt,
      createdAt: input.createdAt,
      updatedAt: input.updatedAt,
      endedAt: input.endedAt,
    });
    return asManagedTaskFlowRecord(flow ?? undefined) ?? null;
  };

  return {
    sessionKey: ownerKey,
    ...(requesterOrigin ? { requesterOrigin } : {}),
    createManaged: (input) => {
      const flow = tryCreateManaged(input);
      if (!flow) {
        throw new Error("TaskFlow persistence failed.");
      }
      return flow;
    },
    tryCreateManaged,
    startManaged: (input) => {
      const result = claimManagedTaskFlowStart({
        ownerKey,
        controllerId: input.controllerId,
        requesterOrigin,
        runId: input.runId,
        toolCallId: input.toolCallId,
        requestJson: input.requestJson,
        runnerLease: input.runnerLease,
        notifyPolicy: input.notifyPolicy,
        goal: input.goal,
        currentStep: input.currentStep,
        stateJson: input.stateJson,
      });
      if (!result.claimed) {
        return {
          ok: false,
          code: result.reason,
          ...(result.current ? { current: result.current } : {}),
        };
      }
      const managed = asManagedTaskFlowRecord(result.flow);
      return managed
        ? { ok: true, created: result.created, flow: managed }
        : { ok: false, code: "persist_failed" };
    },
    get: (flowId) =>
      getTaskFlowByIdForOwner({
        flowId,
        callerOwnerKey: ownerKey,
      }),
    list: () =>
      listTaskFlowsForOwner({
        callerOwnerKey: ownerKey,
      }),
    findLatest: () =>
      findLatestTaskFlowForOwner({
        callerOwnerKey: ownerKey,
      }),
    resolve: (token) =>
      resolveTaskFlowForLookupTokenForOwner({
        token,
        callerOwnerKey: ownerKey,
      }),
    getTaskSummary: (flowId) => {
      const flow = getTaskFlowByIdForOwner({
        flowId,
        callerOwnerKey: ownerKey,
      });
      return flow ? getFlowTaskSummary(flow.flowId) : undefined;
    },
    updateProgress: (input) =>
      applyManagedFlowMutationForOwner({
        flowId: input.flowId,
        ownerKey,
        mutate: (flowId) =>
          updateFlowProgress({
            flowId,
            expectedRevision: input.expectedRevision,
            currentStep: input.currentStep,
            stateJson: input.stateJson,
            updatedAt: input.updatedAt,
          }),
      }),
    setWaiting: (input) =>
      applyManagedFlowMutationForOwner({
        flowId: input.flowId,
        ownerKey,
        mutate: (flowId) =>
          setFlowWaiting({
            flowId,
            expectedRevision: input.expectedRevision,
            currentStep: input.currentStep,
            stateJson: input.stateJson,
            waitJson: input.waitJson,
            blockedTaskId: input.blockedTaskId,
            blockedSummary: input.blockedSummary,
            updatedAt: input.updatedAt,
          }),
      }),
    resume: (input) =>
      applyManagedFlowMutationForOwner({
        flowId: input.flowId,
        ownerKey,
        mutate: (flowId) =>
          resumeFlow({
            flowId,
            expectedRevision: input.expectedRevision,
            status: input.status,
            currentStep: input.currentStep,
            stateJson: input.stateJson,
            runnerLease: input.runnerLease,
            updatedAt: input.updatedAt,
          }),
      }),
    finish: (input) =>
      applyManagedFlowMutationForOwner({
        flowId: input.flowId,
        ownerKey,
        replay: (flow) =>
          matchesTerminalReplay({
            flow,
            expectedRevision: input.expectedRevision,
            status: "succeeded",
            stateJson: input.stateJson,
            updatedAt: input.updatedAt,
            endedAt: input.endedAt,
          }),
        mutate: (flowId) =>
          finishFlow({
            flowId,
            expectedRevision: input.expectedRevision,
            stateJson: input.stateJson,
            updatedAt: input.updatedAt,
            endedAt: input.endedAt,
          }),
      }),
    fail: (input) =>
      applyManagedFlowMutationForOwner({
        flowId: input.flowId,
        ownerKey,
        replay: (flow) =>
          matchesTerminalReplay({
            flow,
            expectedRevision: input.expectedRevision,
            status: "failed",
            stateJson: input.stateJson,
            blockedTaskId: input.blockedTaskId,
            blockedSummary: input.blockedSummary,
            updatedAt: input.updatedAt,
            endedAt: input.endedAt,
          }),
        mutate: (flowId) =>
          failFlow({
            flowId,
            expectedRevision: input.expectedRevision,
            stateJson: input.stateJson,
            blockedTaskId: input.blockedTaskId,
            blockedSummary: input.blockedSummary,
            updatedAt: input.updatedAt,
            endedAt: input.endedAt,
          }),
      }),
    requestCancel: (input) =>
      applyManagedFlowMutationForOwner({
        flowId: input.flowId,
        ownerKey,
        mutate: (flowId) =>
          requestFlowCancel({
            flowId,
            expectedRevision: input.expectedRevision,
            cancelRequestedAt: input.cancelRequestedAt,
          }),
      }),
    cancel: ({ flowId, cfg }) =>
      cancelFlowByIdForOwner({
        cfg,
        flowId,
        callerOwnerKey: ownerKey,
      }),
    runTask: (input) => {
      const created = runTaskInFlowForOwner({
        flowId: input.flowId,
        callerOwnerKey: ownerKey,
        runtime: input.runtime,
        sourceId: input.sourceId,
        childSessionKey: input.childSessionKey,
        parentTaskId: input.parentTaskId,
        agentId: input.agentId,
        runId: input.runId,
        label: input.label,
        task: input.task,
        preferMetadata: input.preferMetadata,
        notifyPolicy: input.notifyPolicy,
        deliveryStatus: input.deliveryStatus,
        status: input.status,
        startedAt: input.startedAt,
        lastEventAt: input.lastEventAt,
        progressSummary: input.progressSummary,
      });
      if (!created.created) {
        return {
          created: false,
          found: created.found,
          reason: created.reason ?? "Task was not created.",
          ...(created.flow ? { flow: created.flow } : {}),
        };
      }
      const managed = asManagedTaskFlowRecord(created.flow);
      if (!managed) {
        return {
          created: false,
          found: true,
          reason: "TaskFlow does not accept managed child tasks.",
          flow: created.flow,
        };
      }
      if (!created.task) {
        return {
          created: false,
          found: true,
          reason: "Task was not created.",
          flow: created.flow,
        };
      }
      return {
        created: true,
        flow: managed,
        task: created.task,
      };
    },
    recordTaskProgress: (input) => {
      const task = resolveManagedFlowTask({
        ownerKey,
        flowId: input.flowId,
        taskId: input.taskId,
      });
      if (!task) {
        return { applied: false, code: "not_found" };
      }
      if (isTerminalTaskStatus(task.status)) {
        return { applied: false, code: "terminal", current: task };
      }
      const updated = updateTaskStateById({
        taskId: task.taskId,
        progressSummary: input.progressSummary,
        eventSummary: input.eventSummary,
        lastEventAt: input.lastEventAt ?? Date.now(),
      });
      return updated
        ? { applied: true, task: updated }
        : { applied: false, code: "persist_failed", current: task };
    },
    finalizeTask: (input) => {
      const task = resolveManagedFlowTask({
        ownerKey,
        flowId: input.flowId,
        taskId: input.taskId,
      });
      if (!task) {
        return { applied: false, code: "not_found" };
      }
      if (isTerminalTaskStatus(task.status)) {
        return task.status === input.status
          ? { applied: true, task }
          : { applied: false, code: "terminal", current: task };
      }
      const endedAt = input.endedAt ?? Date.now();
      const updated = updateTaskStateById({
        taskId: task.taskId,
        status: input.status,
        endedAt,
        lastEventAt: endedAt,
        error: input.error,
        terminalSummary: input.terminalSummary,
        terminalOutcome: input.terminalOutcome,
      });
      return updated
        ? { applied: true, task: updated }
        : { applied: false, code: "persist_failed", current: task };
    },
  };
}

export function createRuntimeTaskFlow(): PluginRuntimeTaskFlow {
  return {
    listRunnerLeaseOrphans: (params) => {
      const ownerId = assertSessionKey(
        params.ownerId,
        "TaskFlow runner orphan query requires an ownerId.",
      );
      const activeLeaseId = assertSessionKey(
        params.activeLeaseId,
        "TaskFlow runner orphan query requires an activeLeaseId.",
      );
      return listTaskFlowRecords().flatMap((flow) => {
        const managed = asManagedTaskFlowRecord(flow);
        return managed &&
          managed.status === "running" &&
          managed.runnerOwnerId === ownerId &&
          managed.runnerLeaseId &&
          managed.runnerLeaseId !== activeLeaseId
          ? [managed]
          : [];
      });
    },
    bindSession: (params) =>
      createBoundTaskFlowRuntime({
        sessionKey: params.sessionKey,
        requesterOrigin: params.requesterOrigin,
      }),
    fromToolContext: (ctx) =>
      createBoundTaskFlowRuntime({
        sessionKey: assertSessionKey(
          ctx.sessionKey,
          "TaskFlow runtime requires tool context with a sessionKey.",
        ),
        requesterOrigin: ctx.deliveryContext,
      }),
  };
}
