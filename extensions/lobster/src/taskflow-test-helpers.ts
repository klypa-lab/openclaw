// Lobster helper module supports taskflow test helpers behavior.
import { vi } from "vitest";
import type { OpenClawPluginApi } from "../runtime-api.js";

type BoundTaskFlow = ReturnType<
  NonNullable<OpenClawPluginApi["runtime"]>["tasks"]["managedFlows"]["bindSession"]
>;

export function createFakeTaskFlow(overrides?: Partial<BoundTaskFlow>): BoundTaskFlow {
  const baseFlow = {
    flowId: "flow-1",
    revision: 1,
    syncMode: "managed" as const,
    controllerId: "tests/lobster",
    ownerKey: "agent:main:main",
    status: "running" as const,
    goal: "Run Lobster workflow",
  };
  const createManaged = vi.fn().mockReturnValue(baseFlow);
  const starts = new Map<string, { request: string; flow: typeof baseFlow }>();
  const startManaged = vi.fn().mockImplementation((input) => {
    const key = `${input.controllerId}\u0000${input.runId}\u0000${input.toolCallId}`;
    const request = JSON.stringify({
      controllerId: input.controllerId,
      goal: input.goal,
      notifyPolicy: input.notifyPolicy ?? null,
      currentStep: input.currentStep ?? null,
      stateJson: input.stateJson ?? null,
      requestJson: input.requestJson ?? null,
    });
    const existing = starts.get(key);
    if (existing) {
      return existing.request === request
        ? { ok: true, created: false, flow: existing.flow }
        : { ok: false, code: "request_conflict", current: existing.flow };
    }
    const flow = {
      ...baseFlow,
      flowId: `flow-${starts.size + 1}`,
      revision: 0,
      status: "running" as const,
      controllerId: input.controllerId,
      goal: input.goal,
      currentStep: input.currentStep,
      stateJson: input.stateJson,
      runnerOwnerId: input.runnerLease.ownerId,
      runnerLeaseId: input.runnerLease.leaseId,
    };
    starts.set(key, { request, flow });
    return { ok: true, created: true, flow };
  });

  return {
    sessionKey: "agent:main:main",
    createManaged,
    tryCreateManaged: vi.fn((params) => createManaged(params)),
    startManaged,
    get: vi.fn(),
    list: vi.fn().mockReturnValue([]),
    findLatest: vi.fn(),
    resolve: vi.fn(),
    getTaskSummary: vi.fn(),
    updateProgress: vi.fn().mockImplementation((input) => ({
      applied: true,
      flow: { ...baseFlow, revision: input.expectedRevision + 1 },
    })),
    setWaiting: vi.fn().mockImplementation((input) => ({
      applied: true,
      flow: { ...baseFlow, revision: input.expectedRevision + 1, status: "waiting" as const },
    })),
    resume: vi.fn().mockImplementation((input) => ({
      applied: true,
      flow: { ...baseFlow, revision: input.expectedRevision + 1, status: "running" as const },
    })),
    finish: vi.fn().mockImplementation((input) => ({
      applied: true,
      flow: { ...baseFlow, revision: input.expectedRevision + 1, status: "succeeded" as const },
    })),
    fail: vi.fn().mockImplementation((input) => ({
      applied: true,
      flow: { ...baseFlow, revision: input.expectedRevision + 1, status: "failed" as const },
    })),
    requestCancel: vi.fn().mockImplementation((input) => ({
      applied: true,
      flow: {
        ...baseFlow,
        revision: input.expectedRevision + 1,
        cancelRequestedAt: Date.now(),
      },
    })),
    cancel: vi.fn(),
    runTask: vi.fn(),
    ...overrides,
  };
}
