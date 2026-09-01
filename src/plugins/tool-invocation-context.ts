import { AsyncLocalStorage } from "node:async_hooks";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import type { OpenClawPluginToolInvocationContext } from "./tool-types.js";

type PluginToolInvocationScope = {
  active: boolean;
  context: OpenClawPluginToolInvocationContext;
};

const pluginToolInvocation = resolveGlobalSingleton<AsyncLocalStorage<PluginToolInvocationScope>>(
  Symbol.for("openclaw.pluginToolInvocation"),
  () => new AsyncLocalStorage(),
);

export function getPluginToolInvocationContext(): OpenClawPluginToolInvocationContext | undefined {
  const scope = pluginToolInvocation.getStore();
  return scope?.active ? scope.context : undefined;
}

export async function runWithPluginToolInvocationContext<T>(
  context: OpenClawPluginToolInvocationContext,
  run: () => T | Promise<T>,
): Promise<T> {
  const scope: PluginToolInvocationScope = {
    active: true,
    context: Object.freeze({ ...context }),
  };
  return await pluginToolInvocation.run(scope, async () => {
    try {
      return await run();
    } finally {
      // Detached async resources inherit the scope object, so close the cell explicitly.
      scope.active = false;
    }
  });
}
