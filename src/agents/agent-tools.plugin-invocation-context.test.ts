import { describe, expect, it, vi } from "vitest";
import { getPluginToolInvocationContext } from "../plugins/tool-invocation-context.js";
import { setPluginToolMeta } from "../plugins/tool-metadata.js";
import {
  rewrapToolWithBeforeToolCallHook,
  wrapToolWithBeforeToolCallHook,
} from "./agent-tools.before-tool-call.js";
import type { AnyAgentTool } from "./agent-tools.types.js";

describe("plugin tool invocation context", () => {
  it("binds each current run to a cached tool and fences detached continuations", async () => {
    const seen: unknown[] = [];
    let inheritedAfterExecute: Promise<unknown> | undefined;
    const source = {
      name: "cached_plugin_tool",
      description: "test",
      parameters: { type: "object", properties: {} },
      execute: vi.fn(async () => {
        seen.push(getPluginToolInvocationContext());
        inheritedAfterExecute = new Promise((resolve) => {
          setImmediate(() => resolve(getPluginToolInvocationContext()));
        });
        return { content: [], details: { ok: true } };
      }),
    } as unknown as AnyAgentTool;
    setPluginToolMeta(source, { pluginId: "cached-plugin", optional: false });
    const prepared = wrapToolWithBeforeToolCallHook(source);

    await rewrapToolWithBeforeToolCallHook(prepared, { runId: "cron-run-1" }).execute("call-1", {});
    await rewrapToolWithBeforeToolCallHook(prepared, { runId: "cron-run-2" }).execute("call-2", {});

    expect(seen).toEqual([
      { runId: "cron-run-1", toolCallId: "call-1" },
      { runId: "cron-run-2", toolCallId: "call-2" },
    ]);
    expect(getPluginToolInvocationContext()).toBeUndefined();
    await expect(inheritedAfterExecute).resolves.toBeUndefined();
  });
});
