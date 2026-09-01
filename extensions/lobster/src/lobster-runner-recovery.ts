// Lobster runner recovery settles work that belonged to a previous Gateway process.
import type { OpenClawPluginApi } from "../runtime-api.js";
import { LOBSTER_RUNNER_LEASE } from "./lobster-taskflow.js";

const RUNNER_STOPPED_SUMMARY = "The Gateway runner stopped before the workflow completed.";

export function recoverOrphanedLobsterFlows(api: OpenClawPluginApi): number {
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
    const result = taskFlow.fail({
      flowId: flow.flowId,
      expectedRevision: flow.revision,
      ...(flow.stateJson !== undefined ? { stateJson: flow.stateJson } : {}),
      blockedSummary: RUNNER_STOPPED_SUMMARY,
    });
    if (result.applied) {
      recovered += 1;
    }
  }
  return recovered;
}
