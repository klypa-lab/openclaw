import { createTestPluginApi } from "openclaw/plugin-sdk/plugin-test-api";
import { createRuntimeTaskFlow } from "openclaw/plugin-sdk/plugin-test-runtime";
import { describe, expect, it, vi } from "vitest";
import plugin from "./index.js";
import type { OpenClawPluginApi } from "./runtime-api.js";

async function triggerGatewayStart(
  managedFlows: ReturnType<typeof createRuntimeTaskFlow>,
  messageAction = vi.fn().mockResolvedValue({ messageId: "message-1" }),
) {
  const on = vi.fn();
  const api = createTestPluginApi({
    id: "lobster",
    name: "lobster",
    on,
    runtime: {
      tasks: {
        managedFlows,
      },
      channel: { outbound: { messageAction } },
    } as unknown as OpenClawPluginApi["runtime"],
  });

  plugin.register(api);
  const hook = on.mock.calls.find(([name]) => name === "gateway_start")?.[1];
  if (typeof hook !== "function") {
    throw new Error("Lobster did not register Gateway restart recovery");
  }
  await hook({ port: 18789 }, {});
  return { messageAction };
}

describe("lobster plugin startup", () => {
  it("fails managed flows owned by a previous Gateway runner lease", async () => {
    const managedFlows = createRuntimeTaskFlow();
    const taskFlow = managedFlows.bindSession({ sessionKey: "agent:main:lobster-recovery" });
    const created = taskFlow.createManaged({
      controllerId: "lobster/run",
      goal: "Run Lobster workflow",
    });
    const started = taskFlow.resume({
      flowId: created.flowId,
      expectedRevision: created.revision,
      status: "running",
      runnerLease: { ownerId: "lobster", leaseId: "lease-before-restart" },
    });
    if (!started.applied) {
      throw new Error("Failed to seed a stale Lobster flow");
    }
    await triggerGatewayStart(managedFlows);

    expect(taskFlow.get(created.flowId)).toMatchObject({
      status: "failed",
      revision: started.flow.revision + 1,
      runnerOwnerId: "lobster",
      runnerLeaseId: "lease-before-restart",
      blockedSummary: "The Gateway runner stopped before the workflow completed.",
    });
  });

  it("preserves a cancellation request across Gateway restart", async () => {
    const managedFlows = createRuntimeTaskFlow();
    const taskFlow = managedFlows.bindSession({ sessionKey: "agent:main:lobster-cancel-recovery" });
    const created = taskFlow.createManaged({
      controllerId: "lobster/run",
      goal: "Run Lobster workflow",
    });
    const started = taskFlow.resume({
      flowId: created.flowId,
      expectedRevision: created.revision,
      status: "running",
      runnerLease: { ownerId: "lobster", leaseId: "lease-before-restart" },
    });
    if (!started.applied) {
      throw new Error("Failed to seed a stale Lobster flow");
    }
    const requested = taskFlow.requestCancel({
      flowId: created.flowId,
      expectedRevision: started.flow.revision,
    });
    if (!requested.applied) {
      throw new Error("Failed to seed a pending cancellation");
    }

    await triggerGatewayStart(managedFlows);

    expect(taskFlow.get(created.flowId)).toMatchObject({
      status: "cancelled",
      revision: requested.flow.revision + 1,
      cancelRequestedAt: requested.flow.cancelRequestedAt,
    });
  });

  it("updates the existing status message when restart recovery settles a flow", async () => {
    const managedFlows = createRuntimeTaskFlow();
    const taskFlow = managedFlows.bindSession({
      sessionKey: "agent:main:lobster-status-recovery",
      requesterOrigin: {
        channel: "telegram",
        to: "123456",
        accountId: "operations",
        threadId: "42",
      },
    });
    const created = taskFlow.createManaged({
      controllerId: "lobster/run",
      goal: "Run Lobster workflow",
    });
    const bound = taskFlow.updateProgress({
      flowId: created.flowId,
      expectedRevision: created.revision,
      stateJson: {
        statusMessage: {
          ownerFlowId: created.flowId,
          messageId: "status-1",
        },
      },
    });
    if (!bound.applied) {
      throw new Error("Failed to seed a Lobster status message binding");
    }
    const started = taskFlow.resume({
      flowId: created.flowId,
      expectedRevision: bound.flow.revision,
      status: "running",
      runnerLease: { ownerId: "lobster", leaseId: "lease-before-restart" },
    });
    if (!started.applied) {
      throw new Error("Failed to seed a stale Lobster flow");
    }

    const messageAction = vi.fn().mockResolvedValue({ messageId: "status-1" });
    await triggerGatewayStart(managedFlows, messageAction);

    expect(messageAction).toHaveBeenCalledOnce();
    expect(messageAction).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "edit",
        accountId: "operations",
        sessionKey: "agent:main:lobster-status-recovery",
        params: expect.objectContaining({
          channel: "telegram",
          to: "123456",
          threadId: "42",
          messageId: "status-1",
          message: expect.stringContaining("Status: Failed"),
        }),
      }),
      { timeoutMs: 90_000 },
    );
  });
});
