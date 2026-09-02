// Lobster runner recovery settles work that belonged to a previous Gateway process.
import type { OpenClawPluginApi } from "../runtime-api.js";
import {
  LOBSTER_RUNNER_LEASE,
  type BoundTaskFlow,
  type ManagedLobsterFlowUpdate,
} from "./lobster-taskflow.js";

const RUNNER_STOPPED_SUMMARY = "The Gateway runner stopped before the workflow completed.";

function isManagedFlow(
  flow: ReturnType<BoundTaskFlow["get"]>,
): flow is Parameters<ManagedLobsterFlowUpdate>[0] {
  return flow?.syncMode === "managed" && typeof flow.controllerId === "string";
}

async function publishRecoveredFlow(
  api: OpenClawPluginApi,
  onFlowUpdate: ManagedLobsterFlowUpdate | undefined,
  flow: Parameters<ManagedLobsterFlowUpdate>[0],
): Promise<void> {
  if (!onFlowUpdate) {
    return;
  }
  try {
    await onFlowUpdate(flow);
  } catch (error) {
    api.logger.warn(
      `Could not update the status message for recovered Lobster workflow ${flow.flowId}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

export async function recoverOrphanedLobsterFlows(
  api: OpenClawPluginApi,
  bindFlowUpdate?: (sessionKey: string, taskFlow: BoundTaskFlow) => ManagedLobsterFlowUpdate,
): Promise<number> {
  const managedFlows = api.runtime?.tasks.managedFlows;
  if (!managedFlows) {
    return 0;
  }
  let recovered = 0;
  const orphans = managedFlows.listRunnerLeaseOrphans({
    ownerId: LOBSTER_RUNNER_LEASE.ownerId,
    activeLeaseId: LOBSTER_RUNNER_LEASE.leaseId,
  });
  for (const flow of orphans) {
    const taskFlow = managedFlows.bindSession({
      sessionKey: flow.ownerKey,
      ...(flow.requesterOrigin ? { requesterOrigin: flow.requesterOrigin } : {}),
    });
    const onFlowUpdate = bindFlowUpdate?.(flow.ownerKey, taskFlow);
    if (flow.cancelRequestedAt != null) {
      const result = await taskFlow.cancel({ flowId: flow.flowId, cfg: api.config });
      if (result.cancelled) {
        recovered += 1;
        const current = taskFlow.get(flow.flowId);
        if (isManagedFlow(current) && onFlowUpdate) {
          await publishRecoveredFlow(api, onFlowUpdate, current);
        }
      } else {
        api.logger.warn(
          `Could not finish cancellation for interrupted Lobster workflow ${flow.flowId}: ${result.reason ?? "unknown error"}`,
        );
      }
      continue;
    }
    const failed = taskFlow.fail({
      flowId: flow.flowId,
      expectedRevision: flow.revision,
      ...(flow.stateJson !== undefined ? { stateJson: flow.stateJson } : {}),
      blockedSummary: RUNNER_STOPPED_SUMMARY,
    });
    if (failed.applied) {
      recovered += 1;
      await publishRecoveredFlow(api, onFlowUpdate, failed.flow);
    }
  }
  return recovered;
}
