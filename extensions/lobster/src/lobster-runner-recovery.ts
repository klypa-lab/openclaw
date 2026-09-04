// Lobster runner recovery settles work that belonged to a previous Gateway process.
import type { OpenClawPluginApi } from "../runtime-api.js";
import { LOBSTER_RUNNER_LEASE, readLobsterTaskBinding } from "./lobster-taskflow.js";

const RUNNER_STOPPED_SUMMARY =
  "The Gateway runner stopped before the workflow completed. Start the workflow again.";

export async function recoverOrphanedLobsterFlows(api: OpenClawPluginApi): Promise<number> {
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
    if (flow.cancelRequestedAt != null) {
      const result = await taskFlow.cancel({ flowId: flow.flowId, cfg: api.config });
      if (result.cancelled) {
        recovered += 1;
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
      const binding = readLobsterTaskBinding(failed.flow);
      if (binding) {
        const finalized = taskFlow.finalizeTask({
          flowId: failed.flow.flowId,
          taskId: binding.taskId,
          status: "failed",
          error: RUNNER_STOPPED_SUMMARY,
          terminalSummary: RUNNER_STOPPED_SUMMARY,
        });
        if (!finalized.applied) {
          api.logger.warn(
            `Could not finalize the task for interrupted Lobster workflow ${failed.flow.flowId}: ${finalized.code}`,
          );
        }
      }
    }
  }
  return recovered;
}
