import { createHash } from "node:crypto";

const SESSION_KEY = "agent:main:clickclack:channel:channel:ch_general";
const LOOP_PROMPT =
  "Run the Lobster commands.list pipeline as a managed TaskFlow and preserve the loop trace key in flow state.";
export const FLOW_CONTROLLER_ID = "lobster/loop";
export const FLOW_GOAL = "Prove packaged /loop to Lobster to TaskFlow execution";

const conversationTag = createHash("sha256").update(SESSION_KEY).digest("hex").slice(0, 12);
const promptHash = createHash("sha256").update(LOOP_PROMPT).digest("hex");

export const LOOP_DECLARATION_KEY = `loop:v1:${conversationTag}:${promptHash}`;
const LOOP_SHORT_NAME = LOOP_PROMPT.slice(0, 40).trimEnd();
export const LOOP_JOB_NAME = `loop[${conversationTag}] ${LOOP_SHORT_NAME}`;
export const LOOP_PAYLOAD_MESSAGE = [
  `[loop ${LOOP_SHORT_NAME}] ${LOOP_PROMPT}`,
  "Do the task and reply concisely. If nothing changed since the last run, reply briefly.",
].join("\n");
export const FLOW_STATE = {
  automation: {
    declarationKey: LOOP_DECLARATION_KEY,
    kind: "loop",
  },
};

function toolCallEvents(name, args, suffix) {
  const serialized = JSON.stringify(args);
  const item = {
    type: "function_call",
    id: `fc_lobster_loop_${suffix}`,
    call_id: `call_lobster_loop_${suffix}`,
    name,
    arguments: serialized,
  };
  return [
    {
      type: "response.output_item.added",
      item: { ...item, arguments: "" },
    },
    { type: "response.function_call_arguments.delta", delta: serialized },
    { type: "response.output_item.done", item },
    {
      type: "response.completed",
      response: {
        id: `resp_lobster_loop_${suffix}`,
        status: "completed",
        output: [item],
        usage: {
          input_tokens: 64,
          output_tokens: 24,
          total_tokens: 88,
          input_tokens_details: { cached_tokens: 0 },
        },
      },
    },
  ];
}

export function buildMockResponses() {
  return [
    {
      events: toolCallEvents(
        "automations",
        {
          action: "add",
          job: {
            name: LOOP_JOB_NAME,
            declarationKey: LOOP_DECLARATION_KEY,
            schedule: { kind: "every", everyMs: 30_000 },
            sessionTarget: "current",
            payload: { kind: "agentTurn", message: LOOP_PAYLOAD_MESSAGE },
          },
        },
        "automation",
      ),
    },
    { text: "Loop created for the managed Lobster workflow." },
    {
      events: toolCallEvents(
        "lobster",
        {
          action: "run",
          pipeline: "commands.list",
          flowControllerId: FLOW_CONTROLLER_ID,
          flowGoal: FLOW_GOAL,
          flowStateJson: JSON.stringify(FLOW_STATE),
          flowCurrentStep: "run_loop_pipeline",
        },
        "lobster",
      ),
    },
    { text: "Managed Lobster workflow started." },
  ];
}
