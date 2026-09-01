import fs from "node:fs";
import {
  applyMockOpenAiModelConfig,
  parseMockOpenAiPort,
} from "../fixtures/mock-openai-config.mjs";
import { buildMockResponses } from "./fixture.mjs";

const [configPath, controlPath, mockPortInput, clickClackPortInput] = process.argv.slice(2);
if (!configPath || !controlPath || !mockPortInput || !clickClackPortInput) {
  throw new Error(
    "usage: write-fixture.mjs <config-path> <response-control-path> <mock-port> <clickclack-port>",
  );
}

const mockPort = parseMockOpenAiPort(mockPortInput);
const clickClackPort = parseMockOpenAiPort(clickClackPortInput);
const config = {
  commands: { text: true, ownerAllowFrom: ["clickclack:usr_human"] },
  gateway: {
    mode: "local",
    bind: "loopback",
    controlUi: { enabled: false },
  },
  agents: {
    entries: {
      main: {
        tools: { allow: ["automations", "lobster"] },
      },
    },
  },
  plugins: {
    enabled: true,
    entries: {
      clickclack: { enabled: true },
      lobster: { enabled: true },
    },
  },
  channels: {
    clickclack: {
      enabled: true,
      baseUrl: `http://127.0.0.1:${clickClackPort}`,
      token: { source: "env", provider: "default", id: "CLICKCLACK_BOT_TOKEN" },
      workspace: "release",
      defaultTo: "channel:general",
      reconnectMs: 250,
    },
  },
};
applyMockOpenAiModelConfig(config, { mockPort });

fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`);
fs.writeFileSync(
  controlPath,
  `${JSON.stringify(
    {
      scriptVersion: "lobster-loop-taskflow-v1",
      responses: buildMockResponses(),
      default: { text: "UNEXPECTED_EXTRA_MODEL_REQUEST" },
    },
    null,
    2,
  )}\n`,
);
