// Defines storage contracts for managed task-flow records.
import type { TaskFlowRecord } from "./task-flow-registry.types.js";

/** Full task-flow registry snapshot used for persistence restore and replacement writes. */
export type TaskFlowRegistryStoreSnapshot = {
  flows: Map<string, TaskFlowRecord>;
};

export type ManagedTaskFlowStartClaim = {
  ownerKey: string;
  controllerId: string;
  runId: string;
  toolCallId: string;
  requestFingerprint: string;
  flow: TaskFlowRecord;
};

export type ManagedTaskFlowStartClaimStoreResult =
  | { claimed: true; created: boolean; flow: TaskFlowRecord }
  | { claimed: false; reason: "request_conflict"; current: TaskFlowRecord };
