import { KeyedAsyncQueue } from "openclaw/plugin-sdk/keyed-async-queue";
import { asOptionalRecord } from "openclaw/plugin-sdk/string-coerce-runtime";
import type { OpenClawPluginApi } from "../runtime-api.js";
import type { BoundTaskFlow, JsonLike } from "./lobster-taskflow.js";

type FlowRecord = NonNullable<ReturnType<BoundTaskFlow["get"]>>;

export type ManagedStatusMessageReceipt = {
  messageId: string;
};

type StatusRoute = {
  channel: string;
  to: string;
  accountId?: string;
  threadId?: string;
};

type DispatchCandidate = {
  flow: FlowRecord;
  taskFlow: BoundTaskFlow;
  sessionKey: string;
  route: StatusRoute;
  messageId: string;
  message: string;
};

const MAX_STATUS_MESSAGE_CHARS = 1_900;
const MAX_REMEMBERED_STATUS_MESSAGES = 256;
const STATUS_MESSAGE_ACTION_TIMEOUT_MS = 90_000;

const STATUS_LABELS: Record<string, string> = {
  queued: "Queued",
  running: "Running",
  waiting: "Waiting",
  blocked: "Blocked",
  succeeded: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  lost: "Stopped unexpectedly",
};

function boundedString(value: unknown, maxLength = 512): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim();
  return normalized && normalized.length <= maxLength ? normalized : undefined;
}

function boundedId(value: unknown): string | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return String(value);
  }
  return boundedString(value);
}

function routeFromFlow(flow: FlowRecord): StatusRoute | undefined {
  const origin = asOptionalRecord(flow.requesterOrigin);
  const channel = boundedString(origin?.channel)?.toLowerCase();
  const to = boundedString(origin?.to);
  const accountId = boundedString(origin?.accountId);
  const threadId = boundedId(origin?.threadId);
  return channel && to
    ? {
        channel,
        to,
        ...(accountId ? { accountId } : {}),
        ...(threadId ? { threadId } : {}),
      }
    : undefined;
}

function bindingFromFlow(flow: FlowRecord): ManagedStatusMessageReceipt | undefined {
  const state = asOptionalRecord(flow.stateJson);
  const binding = asOptionalRecord(state?.statusMessage);
  const messageId = boundedId(binding?.messageId);
  return messageId && binding?.ownerFlowId === flow.flowId ? { messageId } : undefined;
}

function truncate(value: string, maxLength: number): string {
  return value.length <= maxLength ? value : `${value.slice(0, Math.max(0, maxLength - 1))}…`;
}

function renderStatusMessage(flow: FlowRecord): string {
  const status = STATUS_LABELS[flow.status] ?? flow.status;
  const goal = truncate(flow.goal.trim() || "Lobster workflow", 900);
  const step = flow.currentStep ? truncate(flow.currentStep, 800) : undefined;
  return truncate(
    [`Lobster: ${goal}`, `Status: ${status}`, ...(step ? [`Step: ${step}`] : [])].join("\n"),
    MAX_STATUS_MESSAGE_CHARS,
  );
}

function stateRecord(stateJson: JsonLike | undefined): Record<string, JsonLike> {
  if (stateJson && typeof stateJson === "object" && !Array.isArray(stateJson)) {
    return stateJson;
  }
  return stateJson === undefined ? {} : { workflowState: stateJson };
}

export function attachManagedStatusMessage(
  stateJson: JsonLike | undefined,
  ownerFlowId: string,
  receipt: ManagedStatusMessageReceipt,
): JsonLike {
  return {
    ...stateRecord(stateJson),
    statusMessage: {
      ownerFlowId,
      messageId: receipt.messageId,
    },
  };
}

function routeKey(route: StatusRoute, messageId: string): string {
  return [route.channel, route.accountId ?? "", route.to, route.threadId ?? "", messageId].join(
    "\u0000",
  );
}

export function createManagedStatusMessageProjector(api: OpenClawPluginApi) {
  const createByFlowId = new Map<string, Promise<ManagedStatusMessageReceipt | undefined>>();
  const lastDelivered = new Map<string, string>();
  const edits = new KeyedAsyncQueue();

  function rememberDelivered(key: string, message: string): void {
    lastDelivered.delete(key);
    lastDelivered.set(key, message);
    if (lastDelivered.size > MAX_REMEMBERED_STATUS_MESSAGES) {
      const oldest = lastDelivered.keys().next();
      if (!oldest.done) {
        lastDelivered.delete(oldest.value);
      }
    }
  }

  function isAuthoritative(candidate: DispatchCandidate): boolean {
    const fresh = candidate.taskFlow.get(candidate.flow.flowId);
    const freshRoute = fresh ? routeFromFlow(fresh) : undefined;
    const freshBinding = fresh ? bindingFromFlow(fresh) : undefined;
    return (
      fresh?.revision === candidate.flow.revision &&
      freshBinding?.messageId === candidate.messageId &&
      freshRoute?.channel === candidate.route.channel &&
      freshRoute?.accountId === candidate.route.accountId &&
      freshRoute?.to === candidate.route.to &&
      freshRoute?.threadId === candidate.route.threadId
    );
  }

  async function dispatchEdit(key: string, candidate: DispatchCandidate): Promise<void> {
    // Recheck TaskFlow immediately before transport. A newer or terminal
    // revision makes this queued projection stale and therefore a no-op.
    if (!isAuthoritative(candidate) || lastDelivered.get(key) === candidate.message) {
      return;
    }
    await api.runtime.channel.outbound.messageAction(
      {
        channel: candidate.route.channel,
        action: "edit",
        sessionKey: candidate.sessionKey,
        ...(candidate.route.accountId ? { accountId: candidate.route.accountId } : {}),
        idempotencyKey: `lobster-status-${candidate.flow.flowId}-${String(candidate.flow.revision)}`,
        params: {
          channel: candidate.route.channel,
          to: candidate.route.to,
          messageId: candidate.messageId,
          message: candidate.message,
          ...(candidate.route.threadId ? { threadId: candidate.route.threadId } : {}),
        },
      },
      { timeoutMs: STATUS_MESSAGE_ACTION_TIMEOUT_MS },
    );
    rememberDelivered(key, candidate.message);
  }

  function bind(sessionKey: string, taskFlow: BoundTaskFlow) {
    return (flow: FlowRecord): Promise<void> => {
      const route = routeFromFlow(flow);
      const binding = bindingFromFlow(flow);
      if (!route || !binding) {
        return Promise.resolve();
      }
      const key = routeKey(route, binding.messageId);
      return edits.enqueue(key, () =>
        dispatchEdit(key, {
          flow,
          taskFlow,
          sessionKey,
          route,
          messageId: binding.messageId,
          message: renderStatusMessage(flow),
        }),
      );
    };
  }

  function create(
    sessionKey: string,
    flow: FlowRecord,
  ): Promise<ManagedStatusMessageReceipt | undefined> {
    const existing = createByFlowId.get(flow.flowId);
    if (existing) {
      return existing;
    }
    const creation = (async () => {
      const route = routeFromFlow(flow);
      if (!route) {
        return undefined;
      }
      const message = renderStatusMessage(flow);
      const response = asOptionalRecord(
        await api.runtime.channel.outbound.messageAction(
          {
            channel: route.channel,
            action: "send",
            sessionKey,
            ...(route.accountId ? { accountId: route.accountId } : {}),
            idempotencyKey: `lobster-status-create-${flow.flowId}`,
            params: {
              channel: route.channel,
              to: route.to,
              message,
              ...(route.threadId ? { threadId: route.threadId } : {}),
            },
          },
          { timeoutMs: STATUS_MESSAGE_ACTION_TIMEOUT_MS },
        ),
      );
      const messageId =
        boundedId(response?.messageId) ?? boundedId(asOptionalRecord(response?.result)?.messageId);
      if (!messageId) {
        throw new Error("Status message send did not return a messageId");
      }
      const receipt = { messageId };
      rememberDelivered(routeKey(route, messageId), message);
      return receipt;
    })();
    createByFlowId.set(flow.flowId, creation);
    if (createByFlowId.size > 256) {
      const oldest = createByFlowId.keys().next();
      if (!oldest.done && oldest.value !== flow.flowId) {
        createByFlowId.delete(oldest.value);
      }
    }
    return creation;
  }

  return { bind, create };
}
