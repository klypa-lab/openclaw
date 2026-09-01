import fs from "node:fs";
import {
  FLOW_CONTROLLER_ID,
  FLOW_GOAL,
  FLOW_STATE,
  LOOP_DECLARATION_KEY,
  LOOP_JOB_NAME,
  LOOP_PAYLOAD_MESSAGE,
} from "./fixture.mjs";

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function arrayFrom(value, field) {
  if (Array.isArray(value)) {
    return value;
  }
  if (value && typeof value === "object" && Array.isArray(value[field])) {
    return value[field];
  }
  throw new Error(`${field} output is not an array`);
}

function findLoopJob(filePath) {
  const jobs = arrayFrom(readJson(filePath), "jobs");
  const matches = jobs.filter((job) => job?.declarationKey === LOOP_DECLARATION_KEY);
  if (matches.length !== 1) {
    throw new Error(`expected one loop job, found ${matches.length}`);
  }
  const job = matches[0];
  if (job.name !== LOOP_JOB_NAME) {
    throw new Error(`loop job name mismatch: ${JSON.stringify(job.name)}`);
  }
  if (job.sessionTarget !== "current") {
    throw new Error(`loop session target mismatch: ${JSON.stringify(job.sessionTarget)}`);
  }
  if (job.payload?.kind !== "agentTurn" || job.payload?.message !== LOOP_PAYLOAD_MESSAGE) {
    throw new Error("loop payload does not match the /loop work order");
  }
  if (!job.id || typeof job.id !== "string") {
    throw new Error("loop job is missing its id");
  }
  return job;
}

function matchingFlows(filePath) {
  const flows = arrayFrom(readJson(filePath), "flows");
  return flows.filter(
    (flow) => flow?.controllerId === FLOW_CONTROLLER_ID && flow?.goal === FLOW_GOAL,
  );
}

function readRequestBodies(filePath) {
  return fs
    .readFileSync(filePath, "utf8")
    .split(/\r?\n/u)
    .filter(Boolean)
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.path === "/v1/responses")
    .map((entry) =>
      typeof entry.body === "string" ? entry.body : JSON.stringify(entry.body ?? ""),
    );
}

function assertRunSucceeded(filePath) {
  const run = readJson(filePath);
  const completionStatus = run.completionStatus ?? run.status ?? run.result?.completionStatus;
  if (completionStatus !== "succeeded") {
    throw new Error(`automation run did not succeed: ${JSON.stringify(run)}`);
  }
}

function assertFlow(filePath) {
  const value = readJson(filePath);
  const flow = value.flow ?? value;
  if (flow.controllerId !== FLOW_CONTROLLER_ID || flow.goal !== FLOW_GOAL) {
    throw new Error("TaskFlow controller or goal does not match the Lobster invocation");
  }
  if (flow.status !== "succeeded" || typeof flow.endedAt !== "number") {
    throw new Error(`TaskFlow is not terminal: ${JSON.stringify(flow)}`);
  }
  if (flow.currentStep !== "commands_list" || flow.revision !== 2) {
    throw new Error(`TaskFlow progress was not preserved: ${JSON.stringify(flow)}`);
  }
  if (JSON.stringify(flow.stateJson) !== JSON.stringify(FLOW_STATE)) {
    throw new Error(`TaskFlow trace state mismatch: ${JSON.stringify(flow.stateJson)}`);
  }
}

function assertProviderTrace(filePath) {
  const bodies = readRequestBodies(filePath);
  if (bodies.length !== 4) {
    throw new Error(`expected four model requests, got ${bodies.length}`);
  }
  const firstBody = bodies[0];
  const hasWorkOrder = firstBody.includes("Create a recurring loop");
  const hasDeclarationKey = firstBody.includes(LOOP_DECLARATION_KEY);
  if (!hasWorkOrder || !hasDeclarationKey) {
    throw new Error(
      `first model request did not contain the /loop automation work order (chars=${firstBody.length}, workOrder=${hasWorkOrder}, declarationKey=${hasDeclarationKey}, preview=${JSON.stringify(firstBody.slice(0, 160))})`,
    );
  }
  if (!LOOP_PAYLOAD_MESSAGE.split("\n").every((line) => bodies[2].includes(line))) {
    throw new Error("scheduled model request did not contain the stored loop payload");
  }
  if (!bodies[2].includes('"name":"lobster"')) {
    throw new Error("scheduled model request did not expose the Lobster tool");
  }
}

const [command, ...args] = process.argv.slice(2);
if (command === "job-id") {
  process.stdout.write(`${findLoopJob(args[0]).id}\n`);
} else if (command === "flow-id-if-terminal") {
  const matches = matchingFlows(args[0]);
  if (matches.length > 1) {
    throw new Error(`expected at most one managed Lobster flow, found ${matches.length}`);
  }
  const flow = matches[0];
  if (!flow || flow.status !== "succeeded") {
    process.exit(2);
  }
  if (!flow.flowId || typeof flow.flowId !== "string") {
    throw new Error("terminal TaskFlow is missing its flow id");
  }
  process.stdout.write(`${flow.flowId}\n`);
} else if (command === "verify") {
  const [jobsPath, runPath, flowPath, requestLogPath] = args;
  findLoopJob(jobsPath);
  assertRunSucceeded(runPath);
  assertFlow(flowPath);
  assertProviderTrace(requestLogPath);
  console.log("Packaged /loop -> Automation -> Lobster -> TaskFlow proof passed.");
} else {
  throw new Error("usage: assertions.mjs <job-id|flow-id-if-terminal|verify> <artifact paths...>");
}
