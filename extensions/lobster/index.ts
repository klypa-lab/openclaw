// Lobster plugin entrypoint registers its OpenClaw integration.
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import type { AnyAgentTool, OpenClawPluginApi, OpenClawPluginToolFactory } from "./runtime-api.js";
import { recoverOrphanedLobsterFlows } from "./src/lobster-runner-recovery.js";
import { createLobsterTool } from "./src/lobster-tool.js";
import { createManagedStatusMessageProjector } from "./src/status-message.js";

export default definePluginEntry({
  id: "lobster",
  name: "Lobster",
  description: "Optional local shell helper tools",
  register(api: OpenClawPluginApi) {
    const statusMessages = createManagedStatusMessageProjector(api);
    api.on("gateway_start", async () => {
      const recovered = await recoverOrphanedLobsterFlows(api, statusMessages.bind);
      if (recovered > 0) {
        api.logger.warn(`Reconciled ${recovered} interrupted Lobster workflow(s).`);
      }
    });
    api.registerTool(
      ((ctx) => {
        if (ctx.sandboxed) {
          return null;
        }
        const sessionKey = ctx.sessionKey;
        const taskFlow = sessionKey
          ? api.runtime.tasks.managedFlows.fromToolContext(ctx)
          : undefined;
        return createLobsterTool(api, {
          taskFlow,
          getInvocationContext: ctx.getInvocationContext,
          ...(taskFlow && sessionKey
            ? {
                createStatusMessage: (flow) => statusMessages.create(sessionKey, flow),
                onFlowUpdate: statusMessages.bind(sessionKey, taskFlow),
              }
            : {}),
        }) as AnyAgentTool;
      }) as OpenClawPluginToolFactory,
      { optional: true },
    );
  },
});
