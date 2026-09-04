// Runtime task-flow tests cover plugin task-flow registration and execution behavior.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createAcpTaskBackingDetailForTest } from "../../tasks/task-backing-authority.test-support.js";
import { createRunningTaskRunCore } from "../../tasks/task-executor.js";
import { createTaskFlowForTask, getTaskFlowById } from "../../tasks/task-flow-registry.js";
import { getTaskById } from "../../tasks/task-registry.js";
import { getInspectableActiveTaskRestartBlockers } from "../../tasks/task-registry.maintenance.js";
import {
  getRuntimeTaskMocks,
  installRuntimeTaskDeliveryMock,
  resetRuntimeTaskTestState,
} from "./runtime-task-test-harness.js";
import { createRuntimeTaskFlow } from "./runtime-taskflow.js";

function requireCreatedFlow<T>(flow: T | null): T {
  if (!flow) {
    throw new Error("expected managed TaskFlow creation to succeed");
  }
  return flow;
}

afterEach(() => {
  resetRuntimeTaskTestState();
});

describe("runtime TaskFlow", () => {
  beforeEach(() => {
    installRuntimeTaskDeliveryMock();
  });

  it("binds managed TaskFlow operations to a session key", () => {
    const runtime = createRuntimeTaskFlow();
    const taskFlow = runtime.bindSession({
      sessionKey: "agent:main:main",
      requesterOrigin: {
        channel: "telegram",
        to: "telegram:123",
      },
    });

    const created = requireCreatedFlow(
      taskFlow.createManaged({
        controllerId: "tests/runtime-taskflow",
        goal: "Triage inbox",
        currentStep: "classify",
        stateJson: { lane: "inbox" },
      }),
    );

    expect(created.syncMode).toBe("managed");
    expect(created.ownerKey).toBe("agent:main:main");
    expect(created.controllerId).toBe("tests/runtime-taskflow");
    expect(created.requesterOrigin?.channel).toBe("telegram");
    expect(created.requesterOrigin?.to).toBe("telegram:123");
    expect(created.goal).toBe("Triage inbox");
    expect(taskFlow.get(created.flowId)?.flowId).toBe(created.flowId);
    expect(taskFlow.findLatest()?.flowId).toBe(created.flowId);
    expect(taskFlow.resolve("agent:main:main")?.flowId).toBe(created.flowId);
  });

  it("binds TaskFlows from trusted tool context", () => {
    const runtime = createRuntimeTaskFlow();
    const taskFlow = runtime.fromToolContext({
      sessionKey: "agent:main:main",
      deliveryContext: {
        channel: "discord",
        to: "channel:123",
        threadId: "thread:456",
      },
    });

    const created = requireCreatedFlow(
      taskFlow.createManaged({
        controllerId: "tests/runtime-taskflow",
        goal: "Review queue",
      }),
    );

    expect(created.requesterOrigin?.channel).toBe("discord");
    expect(created.requesterOrigin?.to).toBe("channel:123");
    expect(created.requesterOrigin?.threadId).toBe("thread:456");
  });

  it("starts one owner-bound managed flow for an exact invocation", () => {
    const runtime = createRuntimeTaskFlow();
    const taskFlow = runtime.bindSession({ sessionKey: "agent:main:main" });
    const input = {
      controllerId: "tests/runtime-taskflow/start",
      goal: "Run once",
      currentStep: "run_lobster",
      runId: "run-1",
      toolCallId: "call-1",
      requestJson: { pipeline: "noop" },
      runnerLease: { ownerId: "lobster", leaseId: "lease-1" },
    };

    const first = taskFlow.startManaged(input);
    expect(first).toMatchObject({
      ok: true,
      created: true,
      flow: { status: "running", revision: 0, runnerLeaseId: "lease-1" },
    });
    const replay = taskFlow.startManaged({
      ...input,
      runnerLease: { ownerId: "lobster", leaseId: "lease-replay" },
    });
    expect(replay).toMatchObject({ ok: true, created: false });
    if (!first.ok || !replay.ok) {
      throw new Error("Expected exact managed starts to resolve");
    }
    expect(replay.flow.flowId).toBe(first.flow.flowId);
    expect(replay.flow.runnerLeaseId).toBe("lease-1");
    expect(taskFlow.startManaged({ ...input, requestJson: { pipeline: "changed" } })).toMatchObject(
      { ok: false, code: "request_conflict" },
    );

    const otherOwner = runtime.bindSession({ sessionKey: "agent:main:other" });
    const isolated = otherOwner.startManaged(input);
    expect(isolated).toMatchObject({ ok: true, created: true });
    if (!isolated.ok) {
      throw new Error("Expected another owner to receive an isolated flow");
    }
    expect(isolated.flow.flowId).not.toBe(first.flow.flowId);
  });

  it("lists only stale running leases for the exact runner owner", () => {
    const runtime = createRuntimeTaskFlow();
    const taskFlow = runtime.bindSession({ sessionKey: "agent:main:main" });
    const createRunningFlow = (goal: string, ownerId: string, leaseId: string) => {
      const created = requireCreatedFlow(
        taskFlow.createManaged({ controllerId: "tests/runner-lease", goal }),
      );
      const resumed = taskFlow.resume({
        flowId: created.flowId,
        expectedRevision: created.revision,
        status: "running",
        runnerLease: { ownerId, leaseId },
      });
      if (!resumed.applied) {
        throw new Error("expected runner lease to apply");
      }
      return resumed.flow;
    };

    const stale = createRunningFlow("Stale Lobster flow", "lobster", "lease-before-restart");
    createRunningFlow("Current Lobster flow", "lobster", "lease-current");
    createRunningFlow("Other runner flow", "other-plugin", "lease-before-restart");
    const waiting = createRunningFlow("Waiting Lobster flow", "lobster", "lease-before-restart");
    const waitingResult = taskFlow.setWaiting({
      flowId: waiting.flowId,
      expectedRevision: waiting.revision,
    });
    expect(waitingResult.applied).toBe(true);

    expect(
      runtime
        .listRunnerLeaseOrphans({ ownerId: "lobster", activeLeaseId: "lease-current" })
        .map((flow) => flow.flowId),
    ).toEqual([stale.flowId]);

    const queued = requireCreatedFlow(
      taskFlow.createManaged({ controllerId: "tests/runner-lease", goal: "Queued flow" }),
    );
    expect(() =>
      taskFlow.resume({
        flowId: queued.flowId,
        expectedRevision: queued.revision,
        runnerLease: { ownerId: "lobster", leaseId: "lease-current" },
      }),
    ).toThrow("TaskFlow runner lease requires running status.");
    expect(taskFlow.get(queued.flowId)).toMatchObject({ status: "queued", revision: 0 });
  });

  it("rejects tool contexts without a bound session key", () => {
    const runtime = createRuntimeTaskFlow();
    expect(() =>
      runtime.fromToolContext({
        sessionKey: undefined,
        deliveryContext: undefined,
      }),
    ).toThrow("TaskFlow runtime requires tool context with a sessionKey.");
  });

  it("keeps TaskFlow reads owner-scoped and runs child tasks under the bound TaskFlow", () => {
    const runtime = createRuntimeTaskFlow();
    const ownerTaskFlow = runtime.bindSession({
      sessionKey: "agent:main:main",
    });
    const otherTaskFlow = runtime.bindSession({
      sessionKey: "agent:main:other",
    });

    const created = requireCreatedFlow(
      ownerTaskFlow.createManaged({
        controllerId: "tests/runtime-taskflow",
        goal: "Inspect PR batch",
      }),
    );

    expect(otherTaskFlow.get(created.flowId)).toBeUndefined();
    expect(otherTaskFlow.list()).toStrictEqual([]);

    createRunningTaskRunCore({
      runtime: "acp",
      ownerKey: "agent:main:main",
      scopeKind: "session",
      childSessionKey: "agent:main:subagent:child",
      runId: "runtime-taskflow-child",
      task: "Inspect PR 1",
      startedAt: 10,
      detail: createAcpTaskBackingDetailForTest("instance:runtime-taskflow-child"),
    });

    const child = ownerTaskFlow.runTask({
      flowId: created.flowId,
      runtime: "acp",
      childSessionKey: "agent:main:subagent:child",
      runId: "runtime-taskflow-child",
      task: "Inspect PR 1",
      status: "running",
      startedAt: 10,
      lastEventAt: 10,
    });

    expect(child.created).toBe(true);
    if (!child.created) {
      throw new Error("expected child task creation to succeed");
    }
    expect(child.flow.flowId).toBe(created.flowId);
    expect(child.task.parentFlowId).toBe(created.flowId);
    expect(child.task.ownerKey).toBe("agent:main:main");
    expect(child.task.runId).toBe("runtime-taskflow-child");

    const storedTask = getTaskById(child.task.taskId);
    expect(storedTask?.parentFlowId).toBe(created.flowId);
    expect(storedTask?.ownerKey).toBe("agent:main:main");
    expect(getTaskFlowById(created.flowId)?.flowId).toBe(created.flowId);
    const summary = ownerTaskFlow.getTaskSummary(created.flowId);
    if (!summary) {
      throw new Error("expected task summary for created flow");
    }
    expect(summary.total).toBe(1);
    expect(summary.active).toBe(1);
  });

  it("updates, delivers, and finalizes only tasks owned by the bound managed flow", async () => {
    const { sendMessageMock } = getRuntimeTaskMocks();
    sendMessageMock.mockResolvedValue({
      channel: "telegram",
      to: "telegram:123",
      via: "direct",
    });
    const runtime = createRuntimeTaskFlow();
    const taskFlow = runtime.bindSession({
      sessionKey: "agent:main:main",
      requesterOrigin: { channel: "telegram", to: "telegram:123" },
    });
    const otherTaskFlow = runtime.bindSession({ sessionKey: "agent:main:other" });
    const flow = requireCreatedFlow(
      taskFlow.createManaged({
        controllerId: "tests/runtime-taskflow/task-lifecycle",
        goal: "Run managed work",
      }),
    );
    const created = taskFlow.runTask({
      flowId: flow.flowId,
      runtime: "cli",
      task: "Managed controller run",
      label: "Managed work",
      status: "running",
      notifyPolicy: "done_only",
      progressSummary: "Starting",
    });
    if (!created.created) {
      throw new Error("expected managed task creation to succeed");
    }

    expect(
      otherTaskFlow.recordTaskProgress({
        flowId: flow.flowId,
        taskId: created.task.taskId,
        progressSummary: "Forged progress",
      }),
    ).toEqual({ applied: false, code: "not_found" });

    expect(
      taskFlow.recordTaskProgress({
        flowId: flow.flowId,
        taskId: created.task.taskId,
        progressSummary: "Verifying",
        eventSummary: "Verification started",
        lastEventAt: 20,
      }),
    ).toMatchObject({
      applied: true,
      task: { status: "running", progressSummary: "Verifying", lastEventAt: 20 },
    });

    const finalized = taskFlow.finalizeTask({
      flowId: flow.flowId,
      taskId: created.task.taskId,
      status: "succeeded",
      terminalSummary: "Managed work completed.",
      endedAt: 30,
    });
    expect(finalized).toMatchObject({
      applied: true,
      task: {
        status: "succeeded",
        terminalSummary: "Managed work completed.",
        endedAt: 30,
      },
    });
    await vi.waitFor(() => expect(sendMessageMock).toHaveBeenCalledOnce());
    expect(sendMessageMock).toHaveBeenCalledWith(
      expect.objectContaining({
        channel: "telegram",
        to: "telegram:123",
        content: expect.stringContaining("Background task done: Managed work"),
      }),
    );
    expect(
      taskFlow.finalizeTask({
        flowId: flow.flowId,
        taskId: created.task.taskId,
        status: "succeeded",
      }),
    ).toMatchObject({
      applied: true,
      task: {
        taskId: created.task.taskId,
        status: "succeeded",
        terminalSummary: "Managed work completed.",
      },
    });
    expect(sendMessageMock).toHaveBeenCalledOnce();
    expect(
      taskFlow.recordTaskProgress({
        flowId: flow.flowId,
        taskId: created.task.taskId,
        progressSummary: "Late progress",
      }),
    ).toMatchObject({ applied: false, code: "terminal" });
  });

  it("keeps terminal state monotonic and replays the same successful completion", () => {
    const taskFlow = createRuntimeTaskFlow().bindSession({ sessionKey: "agent:main:main" });
    const created = requireCreatedFlow(
      taskFlow.createManaged({
        controllerId: "tests/runtime-taskflow/transitions",
        goal: "Apply transitions",
      }),
    );
    const waiting = taskFlow.setWaiting({
      flowId: created.flowId,
      expectedRevision: 0,
      currentStep: "await_review",
      stateJson: { phase: "waiting" },
      waitJson: { kind: "approval" },
      blockedTaskId: "task-review",
      blockedSummary: "Review required",
      updatedAt: 20,
    });
    expect(waiting).toMatchObject({ applied: true, flow: { revision: 1, status: "blocked" } });
    const resumed = taskFlow.resume({
      flowId: created.flowId,
      expectedRevision: 1,
      status: "running",
      currentStep: "continue_work",
      stateJson: { phase: "running" },
      updatedAt: 30,
    });
    expect(resumed).toMatchObject({ applied: true, flow: { revision: 2, status: "running" } });

    const completion = {
      flowId: created.flowId,
      expectedRevision: 2,
      stateJson: { phase: "done" },
      updatedAt: 40,
      endedAt: 41,
    } as const;
    const finished = taskFlow.finish(completion);
    expect(finished).toMatchObject({
      applied: true,
      flow: { revision: 3, status: "succeeded", stateJson: { phase: "done" } },
    });
    expect(taskFlow.finish(completion)).toEqual(finished);
    expect(getTaskFlowById(created.flowId)?.revision).toBe(3);

    const changedReplay = taskFlow.finish({
      ...completion,
      stateJson: { phase: "different" },
    });
    expect(changedReplay).toMatchObject({
      applied: false,
      code: "revision_conflict",
      current: { revision: 3, status: "succeeded" },
    });

    const terminalMutations = [
      taskFlow.setWaiting({ flowId: created.flowId, expectedRevision: 3 }),
      taskFlow.resume({ flowId: created.flowId, expectedRevision: 3 }),
      taskFlow.fail({ flowId: created.flowId, expectedRevision: 3 }),
      taskFlow.requestCancel({ flowId: created.flowId, expectedRevision: 3 }),
    ];
    for (const mutation of terminalMutations) {
      expect(mutation).toMatchObject({
        applied: false,
        code: "revision_conflict",
        current: { revision: 3, status: "succeeded" },
      });
    }
    expect(getTaskFlowById(created.flowId)).toMatchObject({ revision: 3, status: "succeeded" });
  });

  it("updates progress without changing lifecycle state and rejects late progress", () => {
    const taskFlow = createRuntimeTaskFlow().bindSession({ sessionKey: "agent:main:progress" });
    const started = taskFlow.startManaged({
      controllerId: "tests/runtime-taskflow/progress",
      goal: "Track durable progress",
      currentStep: "prepare",
      runId: "run-progress",
      toolCallId: "call-progress",
      runnerLease: { ownerId: "lobster", leaseId: "lease-progress" },
    });
    if (!started.ok) {
      throw new Error("expected managed TaskFlow start to succeed");
    }

    const progress = taskFlow.updateProgress({
      flowId: started.flow.flowId,
      expectedRevision: 0,
      currentStep: "verify",
      stateJson: { completed: 1, total: 2 },
      updatedAt: 20,
    });
    expect(progress).toMatchObject({
      applied: true,
      flow: {
        revision: 1,
        status: "running",
        currentStep: "verify",
        stateJson: { completed: 1, total: 2 },
        runnerOwnerId: "lobster",
        runnerLeaseId: "lease-progress",
      },
    });

    const finished = taskFlow.finish({
      flowId: started.flow.flowId,
      expectedRevision: 1,
      updatedAt: 30,
      endedAt: 30,
    });
    expect(finished).toMatchObject({
      applied: true,
      flow: { revision: 2, status: "succeeded", currentStep: "verify" },
    });
    expect(
      taskFlow.updateProgress({
        flowId: started.flow.flowId,
        expectedRevision: 2,
        currentStep: "late",
      }),
    ).toMatchObject({
      applied: false,
      code: "revision_conflict",
      current: { revision: 2, status: "succeeded", currentStep: "verify" },
    });
    expect(taskFlow.get(started.flow.flowId)).toMatchObject({
      revision: 2,
      status: "succeeded",
      currentStep: "verify",
    });
  });

  it("replays the same failed completion without changing its revision", () => {
    const taskFlow = createRuntimeTaskFlow().bindSession({ sessionKey: "agent:main:main" });
    const created = requireCreatedFlow(
      taskFlow.createManaged({
        controllerId: "tests/runtime-taskflow/failure-replay",
        goal: "Preserve failure",
      }),
    );
    const completion = {
      flowId: created.flowId,
      expectedRevision: 0,
      stateJson: { phase: "failed" },
      blockedTaskId: "task-failed",
      blockedSummary: "Task failed",
      updatedAt: 50,
      endedAt: 51,
    } as const;
    const failed = taskFlow.fail(completion);
    expect(failed).toMatchObject({ applied: true, flow: { revision: 1, status: "failed" } });
    expect(taskFlow.fail(completion)).toEqual(failed);
    expect(taskFlow.finish({ flowId: created.flowId, expectedRevision: 1 })).toMatchObject({
      applied: false,
      code: "revision_conflict",
      current: { revision: 1, status: "failed" },
    });
    expect(getTaskFlowById(created.flowId)).toMatchObject({ revision: 1, status: "failed" });
  });

  it("rejects invalid mutation targets before writing and preserves conflict mapping", () => {
    const runtime = createRuntimeTaskFlow();
    const ownerTaskFlow = runtime.bindSession({ sessionKey: "agent:main:main" });
    const otherTaskFlow = runtime.bindSession({ sessionKey: "agent:main:other" });
    const managed = requireCreatedFlow(
      ownerTaskFlow.createManaged({
        controllerId: "tests/runtime-taskflow/auth",
        goal: "Keep ownership",
      }),
    );

    const denied = otherTaskFlow.setWaiting({
      flowId: managed.flowId,
      expectedRevision: managed.revision,
    });
    expect(denied).toEqual({ applied: false, code: "not_found" });
    expect(getTaskFlowById(managed.flowId)?.revision).toBe(0);

    const mirrored = requireCreatedFlow(
      createTaskFlowForTask({
        task: {
          ownerKey: "agent:main:main",
          taskId: "task-mirrored",
          notifyPolicy: "done_only",
          status: "running",
          task: "Mirror this task",
          createdAt: 10,
          lastEventAt: 10,
        },
      }),
    );
    const wrongMode = ownerTaskFlow.resume({
      flowId: mirrored.flowId,
      expectedRevision: mirrored.revision,
    });
    expect(wrongMode).toMatchObject({ applied: false, code: "not_managed" });

    const conflict = ownerTaskFlow.finish({ flowId: managed.flowId, expectedRevision: 1 });
    expect(conflict).toMatchObject({ applied: false, code: "revision_conflict" });
    expect(getTaskFlowById(managed.flowId)).toMatchObject({
      revision: 0,
      status: "queued",
    });
    expect(getTaskFlowById(managed.flowId)?.endedAt).toBeUndefined();
    expect(getTaskFlowById(mirrored.flowId)?.revision).toBe(0);
  });

  // Declared last on purpose: it observes what the earlier tests' afterEach
  // resets left behind. A reset that keeps durable rows lets
  // ensureTaskRegistryReady() restore them here, and every later test file in
  // this isolate:false worker then inherits phantom active restart blockers.
  it("leaves no restorable task restart blockers for later test files", () => {
    expect(getInspectableActiveTaskRestartBlockers()).toStrictEqual([]);
  });
});
