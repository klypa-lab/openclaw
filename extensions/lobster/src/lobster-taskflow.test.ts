// Lobster tests cover lobster taskflow plugin behavior.
import { createRuntimeTaskFlow } from "openclaw/plugin-sdk/plugin-test-runtime";
import { describe, expect, it, vi } from "vitest";
import type { LobsterRunner } from "./lobster-runner.js";
import {
  LOBSTER_RUNNER_LEASE,
  launchManagedLobsterFlow,
  type ManagedLobsterFlowResult,
  resumeManagedLobsterFlow,
} from "./lobster-taskflow.js";
import { createFakeTaskFlow } from "./taskflow-test-helpers.js";

function expectManagedFlowFailure(result: ManagedLobsterFlowResult) {
  expect(result.ok).toBe(false);
  if (result.ok) {
    throw new Error("Expected managed Lobster flow to fail");
  }
  return result;
}

async function runManagedLobsterFlow(
  params: Parameters<typeof launchManagedLobsterFlow>[0],
): Promise<ManagedLobsterFlowResult> {
  const launched = launchManagedLobsterFlow(params);
  if (!launched.ok) {
    return launched;
  }
  return launched.created
    ? await launched.completion
    : { ok: true, replayed: true, flow: launched.flow };
}

function createRunner(result: Awaited<ReturnType<LobsterRunner["run"]>>): LobsterRunner {
  return {
    run: vi.fn().mockResolvedValue(result),
  };
}

function createRunFlowParams(
  taskFlow: ReturnType<typeof createFakeTaskFlow>,
  runner: LobsterRunner,
): Parameters<typeof runManagedLobsterFlow>[0] {
  return {
    taskFlow,
    config: {},
    runner,
    runnerParams: {
      action: "run",
      pipeline: "noop",
      cwd: process.cwd(),
      timeoutMs: 1000,
      maxStdoutBytes: 4096,
    },
    controllerId: "tests/lobster",
    goal: "Run Lobster workflow",
    invocation: { runId: "run-1", toolCallId: "call-1" },
  };
}

function createResumeFlowParams(
  taskFlow: ReturnType<typeof createFakeTaskFlow>,
  runner: LobsterRunner,
): Parameters<typeof resumeManagedLobsterFlow>[0] {
  return {
    taskFlow,
    config: {},
    runner,
    flowId: "flow-1",
    expectedRevision: 4,
    runnerParams: {
      action: "resume",
      token: "resume-1",
      approve: true,
      cwd: process.cwd(),
      timeoutMs: 1000,
      maxStdoutBytes: 4096,
    },
  };
}

describe("runManagedLobsterFlow", () => {
  it("replays one managed start without launching a second runner", async () => {
    const taskFlow = createFakeTaskFlow();
    const runner = createRunner({
      ok: true,
      status: "ok",
      output: [],
      requiresApproval: null,
    });
    const params = {
      ...createRunFlowParams(taskFlow, runner),
      invocation: { runId: "run-1", toolCallId: "call-1" },
    };

    const first = launchManagedLobsterFlow(params);
    const replay = launchManagedLobsterFlow(params);
    expect(first.ok).toBe(true);
    expect(replay.ok).toBe(true);
    if (!first.ok || !replay.ok) {
      throw new Error("Expected both starts to resolve");
    }
    expect(first.created).toBe(true);
    expect(replay.created).toBe(false);
    if (!first.created) {
      throw new Error("Expected the first start to launch");
    }
    await first.completion;

    expect(replay.flow.flowId).toBe(first.flow.flowId);
    expect(runner.run).toHaveBeenCalledOnce();
  });

  it("creates a flow and finishes it when Lobster succeeds", async () => {
    const taskFlow = createFakeTaskFlow();
    const runner = createRunner({
      ok: true,
      status: "ok",
      output: [{ id: "result-1" }],
      requiresApproval: null,
    });

    const result = await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner));

    expect(result.ok).toBe(true);
    expect(taskFlow.startManaged).toHaveBeenCalledWith({
      controllerId: "tests/lobster",
      goal: "Run Lobster workflow",
      runId: "run-1",
      toolCallId: "call-1",
      currentStep: "run_lobster",
      runnerLease: LOBSTER_RUNNER_LEASE,
      requestJson: {
        runnerParams: {
          action: "run",
          pipeline: "noop",
          cwd: process.cwd(),
          timeoutMs: 1000,
          maxStdoutBytes: 4096,
        },
        waitingStep: null,
      },
    });
    expect(taskFlow.finish).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 0,
    });
  });

  it("reports a rejected completion instead of hiding it", async () => {
    const taskFlow = createFakeTaskFlow({
      finish: vi.fn().mockReturnValue({
        applied: false,
        code: "revision_conflict",
      }),
    });
    const runner = createRunner({
      ok: true,
      status: "ok",
      output: [],
      requiresApproval: null,
    });

    const result = expectManagedFlowFailure(
      await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner)),
    );

    expect(result.error.message).toMatch(/revision_conflict/u);
  });

  it("serializes cyclic and supported approval items before waiting", async () => {
    const taskFlow = createFakeTaskFlow();
    const createdAt = new Date("2026-04-05T21:00:00.000Z");
    const selfArray: unknown[] = [];
    selfArray.push(selfArray);
    const objectArrayCycle: Record<string, unknown> = {};
    objectArrayCycle.items = [objectArrayCycle];
    const shared = { id: "shared" };
    const protoEntry: Record<string, unknown> = JSON.parse(
      '{"__proto__":{"polluted":true},"kept":"value"}',
    );
    const runner = createRunner({
      ok: true,
      status: "needs_approval",
      output: [],
      requiresApproval: {
        type: "approval_request",
        prompt: "Approve this?",
        items: [
          {
            selfArray,
            objectArrayCycle,
            repeated: [shared, shared],
            createdAt,
            infinity: Number.POSITIVE_INFINITY,
            count: 2n,
            omitted: {
              kept: true,
              undefinedValue: undefined,
              function: () => true,
              symbol: Symbol("skip"),
            },
            protoEntry,
            arrayValues: [undefined, () => true, Symbol("skip"), Number.NaN],
          },
        ],
        resumeToken: "resume-1",
      },
    });

    const result = await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner));

    expect(result.ok).toBe(true);
    expect(taskFlow.setWaiting).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 0,
      currentStep: "await_lobster_approval",
      waitJson: {
        kind: "lobster_approval",
        prompt: "Approve this?",
        items: [
          {
            selfArray: ["[Circular]"],
            objectArrayCycle: { items: ["[Circular]"] },
            repeated: [{ id: "shared" }, { id: "shared" }],
            createdAt: createdAt.toISOString(),
            infinity: "Infinity",
            count: "2",
            omitted: { kept: true },
            protoEntry: { kept: "value" },
            arrayValues: [null, null, null, "NaN"],
          },
        ],
        resumeToken: "resume-1",
      },
    });
  });

  it("reports a rejected approval wait instead of hiding it", async () => {
    const taskFlow = createFakeTaskFlow({
      setWaiting: vi.fn().mockReturnValue({
        applied: false,
        code: "revision_conflict",
      }),
    });
    const runner = createRunner({
      ok: true,
      status: "needs_approval",
      output: [],
      requiresApproval: {
        type: "approval_request",
        prompt: "Approve this?",
        items: [],
        resumeToken: "resume-1",
      },
    });

    const result = expectManagedFlowFailure(
      await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner)),
    );

    expect(result.error.message).toMatch(/revision_conflict/u);
  });

  it("fails the flow when Lobster returns an error envelope", async () => {
    const taskFlow = createFakeTaskFlow();
    const runner = createRunner({
      ok: false,
      error: {
        type: "runtime_error",
        message: "boom",
      },
    });

    const result = expectManagedFlowFailure(
      await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner)),
    );
    expect(result.error.message).toBe("boom");
    expect(taskFlow.fail).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 0,
    });
  });

  it("fails the flow when the runner throws", async () => {
    const taskFlow = createFakeTaskFlow();
    const runner: LobsterRunner = {
      run: vi.fn().mockRejectedValue(new Error("crashed")),
    };

    const result = expectManagedFlowFailure(
      await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner)),
    );
    expect(result.error.message).toBe("crashed");
    expect(taskFlow.fail).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 0,
    });
  });
});

describe("resumeManagedLobsterFlow", () => {
  it("resumes the flow and finishes it on success", async () => {
    const taskFlow = createFakeTaskFlow();
    const runner = createRunner({
      ok: true,
      status: "ok",
      output: [],
      requiresApproval: null,
    });

    const result = await resumeManagedLobsterFlow(createResumeFlowParams(taskFlow, runner));

    expect(result.ok).toBe(true);
    expect(taskFlow.resume).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 4,
      status: "running",
      currentStep: "resume_lobster",
      runnerLease: LOBSTER_RUNNER_LEASE,
    });
    expect(taskFlow.finish).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 5,
    });
  });

  it("returns a mutation error when taskFlow resume is rejected", async () => {
    const taskFlow = createFakeTaskFlow({
      resume: vi.fn().mockReturnValue({
        applied: false,
        code: "revision_conflict",
      }),
    });
    const runner = createRunner({
      ok: true,
      status: "ok",
      output: [],
      requiresApproval: null,
    });

    const result = expectManagedFlowFailure(
      await resumeManagedLobsterFlow(createResumeFlowParams(taskFlow, runner)),
    );
    expect(result.error.message).toMatch(/revision_conflict/);
    expect(runner.run).not.toHaveBeenCalled();
  });

  it("fails the resumed flow when the runner throws", async () => {
    const taskFlow = createFakeTaskFlow();
    const runner: LobsterRunner = {
      run: vi.fn().mockRejectedValue(new Error("crashed")),
    };

    const result = expectManagedFlowFailure(
      await resumeManagedLobsterFlow(createResumeFlowParams(taskFlow, runner)),
    );

    expect(result.error.message).toBe("crashed");
    expect(taskFlow.fail).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 5,
    });
  });

  it("returns to waiting when the resumed Lobster run needs approval again", async () => {
    const taskFlow = createFakeTaskFlow();
    const runner = createRunner({
      ok: true,
      status: "needs_approval",
      output: [],
      requiresApproval: {
        type: "approval_request",
        prompt: "Approve this too?",
        items: [{ id: "item-2" }],
        resumeToken: "resume-2",
      },
    });

    const result = await resumeManagedLobsterFlow(createResumeFlowParams(taskFlow, runner));

    expect(result.ok).toBe(true);
    expect(taskFlow.setWaiting).toHaveBeenCalledWith({
      flowId: "flow-1",
      expectedRevision: 5,
      currentStep: "await_lobster_approval",
      waitJson: {
        kind: "lobster_approval",
        prompt: "Approve this too?",
        items: [{ id: "item-2" }],
        resumeToken: "resume-2",
      },
    });
  });
});

describe("cancelled managed Lobster flows", () => {
  it.each(["run", "resume"] as const)(
    "does not start Lobster after TaskFlow cancellation wins %s admission",
    async (action) => {
      const cancelledFlow = {
        flowId: "flow-1",
        revision: 1,
        syncMode: "managed" as const,
        controllerId: "tests/lobster",
        ownerKey: "agent:main:main",
        status: "cancelled" as const,
        goal: "Run Lobster workflow",
        cancelRequestedAt: Date.now(),
      };
      const taskFlow = createFakeTaskFlow({
        get: vi.fn().mockReturnValue(cancelledFlow),
      });
      const runner = createRunner({
        ok: true,
        status: "ok",
        output: [],
        requiresApproval: null,
      });

      const result = expectManagedFlowFailure(
        action === "run"
          ? await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner))
          : await resumeManagedLobsterFlow(createResumeFlowParams(taskFlow, runner)),
      );

      expect(result.error.message).toBe("TaskFlow cancellation requested");
      expect(runner.run).not.toHaveBeenCalled();
      expect(taskFlow.finish).not.toHaveBeenCalled();
      expect(taskFlow.fail).not.toHaveBeenCalled();
    },
  );

  it("aborts active Lobster work without replacing the cancelled TaskFlow", async () => {
    const taskFlow = createRuntimeTaskFlow().bindSession({
      sessionKey: "agent:main:lobster-active-cancel",
    });
    const runner: LobsterRunner = {
      run: vi.fn(
        async (params) =>
          await new Promise<never>((_resolve, reject) => {
            const onAbort = () => {
              const reason = params.signal?.reason;
              reject(reason instanceof Error ? reason : new Error("Lobster workflow cancelled"));
            };
            params.signal?.addEventListener("abort", onAbort, { once: true });
          }),
      ),
    };

    const launched = launchManagedLobsterFlow(createRunFlowParams(taskFlow, runner));
    expect(launched.ok).toBe(true);
    if (!launched.ok || !launched.created) {
      throw new Error("Expected managed Lobster flow to start");
    }
    await vi.waitFor(() => expect(runner.run).toHaveBeenCalledOnce());

    await expect(taskFlow.cancel({ flowId: launched.flow.flowId, cfg: {} })).resolves.toMatchObject(
      { cancelled: true },
    );
    const result = expectManagedFlowFailure(await launched.completion);

    expect(result.error.message).toBe("TaskFlow cancellation requested");
    expect(taskFlow.get(launched.flow.flowId)?.status).toBe("cancelled");
  });

  it("observes cancellation that arrives as the runner settles", async () => {
    let cancelled = false;
    const taskFlow = createFakeTaskFlow({
      get: vi.fn(() => ({
        flowId: "flow-1",
        revision: cancelled ? 1 : 0,
        syncMode: "managed" as const,
        controllerId: "tests/lobster",
        ownerKey: "agent:main:main",
        status: cancelled ? ("cancelled" as const) : ("running" as const),
        notifyPolicy: "silent" as const,
        goal: "Run Lobster workflow",
        createdAt: 1,
        updatedAt: cancelled ? 2 : 1,
        ...(cancelled ? { cancelRequestedAt: Date.now() } : {}),
      })),
    });
    const runner: LobsterRunner = {
      run: vi.fn(async () => {
        cancelled = true;
        return {
          ok: true as const,
          status: "ok" as const,
          output: [],
          requiresApproval: null,
        };
      }),
    };

    const result = expectManagedFlowFailure(
      await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner)),
    );

    expect(result.error.message).toBe("TaskFlow cancellation requested");
    expect(taskFlow.finish).not.toHaveBeenCalled();
    expect(taskFlow.fail).not.toHaveBeenCalled();
  });

  it.each(["run", "resume"])(
    "persists a cancelled TaskFlow for a rejected Lobster %s",
    async (action) => {
      const taskFlow = createRuntimeTaskFlow().bindSession({
        sessionKey: `agent:main:lobster-cancel-${action}`,
      });
      const runner = createRunner({
        ok: true,
        status: "cancelled",
        output: [],
        requiresApproval: null,
      });
      let result;
      if (action === "run") {
        result = await runManagedLobsterFlow(createRunFlowParams(taskFlow, runner));
      } else {
        const waitingFlow = taskFlow.createManaged({
          controllerId: "tests/lobster",
          goal: "Resume Lobster workflow",
          status: "waiting",
        });
        result = await resumeManagedLobsterFlow({
          ...createResumeFlowParams(taskFlow, runner),
          flowId: waitingFlow.flowId,
          expectedRevision: waitingFlow.revision,
        });
      }

      if (!result.ok) {
        throw result.error;
      }
      expect(taskFlow.get(result.flow.flowId)?.status).toBe("cancelled");
    },
  );

  it.each(["unsettled", "rejected"])(
    "does not finish or fail when TaskFlow cancellation is %s",
    async (outcome) => {
      const cancel =
        outcome === "unsettled"
          ? vi.fn().mockResolvedValue({
              found: true,
              cancelled: false,
              reason: "One or more child tasks are still active.",
              tasks: [],
            })
          : vi.fn().mockRejectedValue(new Error("cancel transport error"));
      const taskFlow = createFakeTaskFlow({ cancel });
      const runner = createRunner({
        ok: true,
        status: "cancelled",
        output: [],
        requiresApproval: null,
      });

      const result = expectManagedFlowFailure(
        await resumeManagedLobsterFlow(createResumeFlowParams(taskFlow, runner)),
      );

      expect(result.error.message).toMatch(/cancellation failed|cancel transport error/u);
      expect(taskFlow.finish).not.toHaveBeenCalled();
      expect(taskFlow.fail).not.toHaveBeenCalled();
    },
  );
});
