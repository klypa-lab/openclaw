import { createTestPluginApi } from "openclaw/plugin-sdk/plugin-test-api";
import { createRuntimeTaskFlow } from "openclaw/plugin-sdk/plugin-test-runtime";
import { describe, expect, it, vi } from "vitest";
import type { OpenClawPluginApi } from "../runtime-api.js";
import {
  attachManagedStatusMessage,
  createManagedStatusMessageProjector,
} from "./status-message.js";

function createProjector() {
  const messageAction = vi.fn().mockResolvedValue({ messageId: "message-1" });
  const api = createTestPluginApi({
    id: "lobster",
    name: "lobster",
    runtime: {
      channel: { outbound: { messageAction } },
    } as unknown as OpenClawPluginApi["runtime"],
  });
  return { messageAction, projector: createManagedStatusMessageProjector(api) };
}

let flowSequence = 0;

function createFlow() {
  flowSequence += 1;
  const sessionKey = `agent:main:status-message:${String(flowSequence)}`;
  const taskFlow = createRuntimeTaskFlow().bindSession({
    sessionKey,
    requesterOrigin: {
      channel: "telegram",
      to: "-100123",
      threadId: "42",
      accountId: "operations",
    },
  });
  const started = taskFlow.startManaged({
    controllerId: "tests/lobster",
    goal: "Verify the release candidate",
    runId: `run-${String(flowSequence)}`,
    toolCallId: `call-${String(flowSequence)}`,
    currentStep: "build",
    runnerLease: { ownerId: "lobster", leaseId: "lease-1" },
  });
  if (!started.ok) {
    throw new Error(`Failed to create test flow: ${started.code}`);
  }
  return { taskFlow, flow: started.flow, sessionKey };
}

describe("managed Lobster status message", () => {
  it("creates one portable message after the durable flow identity exists", async () => {
    const { messageAction, projector } = createProjector();
    const { flow, sessionKey } = createFlow();

    const [first, replay] = await Promise.all([
      projector.create(sessionKey, flow),
      projector.create(sessionKey, flow),
    ]);

    expect(first).toEqual({ messageId: "message-1" });
    expect(replay).toEqual(first);
    expect(messageAction).toHaveBeenCalledOnce();
    expect(messageAction).toHaveBeenCalledWith(
      {
        channel: "telegram",
        action: "send",
        sessionKey,
        accountId: "operations",
        idempotencyKey: `lobster-status-create-${flow.flowId}`,
        params: {
          channel: "telegram",
          to: "-100123",
          threadId: "42",
          message: expect.stringMatching(/Status: Running[\s\S]*Step: build/u),
        },
      },
      { timeoutMs: 90_000 },
    );
  });

  it("edits the same message only for authoritative newer revisions", async () => {
    const { messageAction, projector } = createProjector();
    const { taskFlow, flow, sessionKey } = createFlow();
    const receipt = await projector.create(sessionKey, flow);
    if (!receipt) {
      throw new Error("Expected a status-message receipt");
    }
    const bound = taskFlow.updateProgress({
      flowId: flow.flowId,
      expectedRevision: flow.revision,
      currentStep: flow.currentStep,
      stateJson: attachManagedStatusMessage(flow.stateJson, flow.flowId, receipt),
    });
    if (!bound.applied) {
      throw new Error(`Failed to bind status message: ${bound.code}`);
    }
    const running = taskFlow.updateProgress({
      flowId: flow.flowId,
      expectedRevision: bound.flow.revision,
      currentStep: "verify",
    });
    if (!running.applied) {
      throw new Error(`Failed to advance status message: ${running.code}`);
    }
    const update = projector.bind(sessionKey, taskFlow);

    await update(running.flow);
    const terminal = taskFlow.finish({
      flowId: flow.flowId,
      expectedRevision: running.flow.revision,
    });
    if (!terminal.applied) {
      throw new Error(`Failed to finish status message: ${terminal.code}`);
    }
    await update(terminal.flow);
    await update(running.flow);

    expect(messageAction).toHaveBeenCalledTimes(3);
    expect(messageAction).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        action: "edit",
        idempotencyKey: `lobster-status-${flow.flowId}-${running.flow.revision}`,
        params: expect.objectContaining({
          messageId: "message-1",
          message: expect.stringMatching(/Status: Running[\s\S]*Step: verify/u),
        }),
      }),
      { timeoutMs: 90_000 },
    );
    expect(messageAction).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        action: "edit",
        idempotencyKey: `lobster-status-${flow.flowId}-${terminal.flow.revision}`,
        params: expect.objectContaining({
          messageId: "message-1",
          message: expect.stringContaining("Status: Completed"),
        }),
      }),
      { timeoutMs: 90_000 },
    );
  });

  it("keeps only the newest revision waiting behind an active edit", async () => {
    const { messageAction, projector } = createProjector();
    const { taskFlow, flow, sessionKey } = createFlow();
    const receipt = await projector.create(sessionKey, flow);
    if (!receipt) {
      throw new Error("Expected a status-message receipt");
    }
    const bound = taskFlow.updateProgress({
      flowId: flow.flowId,
      expectedRevision: flow.revision,
      stateJson: attachManagedStatusMessage(flow.stateJson, flow.flowId, receipt),
    });
    if (!bound.applied) {
      throw new Error(`Failed to bind status message: ${bound.code}`);
    }
    const first = taskFlow.updateProgress({
      flowId: flow.flowId,
      expectedRevision: bound.flow.revision,
      currentStep: "first",
    });
    if (!first.applied) {
      throw new Error(`Failed to advance status message: ${first.code}`);
    }

    let markActiveEditStarted: (() => void) | undefined;
    const activeEditStarted = new Promise<void>((resolve) => {
      markActiveEditStarted = resolve;
    });
    let releaseActiveEdit: (() => void) | undefined;
    messageAction.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          releaseActiveEdit = () => resolve({ messageId: "message-1" });
          markActiveEditStarted?.();
        }),
    );
    const update = projector.bind(sessionKey, taskFlow);
    const activeEdit = update(first.flow);
    await activeEditStarted;
    const skipped = taskFlow.updateProgress({
      flowId: flow.flowId,
      expectedRevision: first.flow.revision,
      currentStep: "superseded",
    });
    if (!skipped.applied) {
      throw new Error(`Failed to advance status message: ${skipped.code}`);
    }
    const skippedEdit = update(skipped.flow);
    const terminal = taskFlow.finish({
      flowId: flow.flowId,
      expectedRevision: skipped.flow.revision,
    });
    if (!terminal.applied) {
      throw new Error(`Failed to finish status message: ${terminal.code}`);
    }
    const terminalEdit = update(terminal.flow);
    releaseActiveEdit?.();
    await Promise.all([activeEdit, skippedEdit, terminalEdit]);

    expect(messageAction).toHaveBeenCalledTimes(3);
    expect(messageAction).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        idempotencyKey: `lobster-status-${flow.flowId}-${terminal.flow.revision}`,
        params: expect.objectContaining({
          message: expect.stringContaining("Status: Completed"),
        }),
      }),
      { timeoutMs: 90_000 },
    );
  });
});
