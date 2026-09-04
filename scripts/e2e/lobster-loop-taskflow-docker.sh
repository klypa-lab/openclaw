#!/usr/bin/env bash
# Proves the packaged /loop -> Automation -> Lobster -> TaskFlow composition.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/lib/docker-e2e-image.sh"
source "$ROOT_DIR/scripts/e2e/lib/prepublish-plugin-registry.sh"

IMAGE_NAME="$(docker_e2e_resolve_image "openclaw-lobster-loop-taskflow-e2e" OPENCLAW_IMAGE)"
PORT="18789"
MOCK_PORT="44082"
CLICKCLACK_PORT="44083"
TOKEN="lobster-loop-taskflow-e2e-token"
CONTAINER_NAME="openclaw-lobster-loop-taskflow-e2e-$$"
RUN_LOG="$(docker_e2e_run_log lobster-loop-taskflow)"
OPENCLAW_PREPUBLISH_PLUGIN_REGISTRY_DOCKER_ARGS=()

if [ -n "${OPENCLAW_PREPUBLISH_PLUGIN_REGISTRY_DIR:-}" ]; then
  openclaw_prepublish_plugin_registry_configure_docker_args \
    "$OPENCLAW_PREPUBLISH_PLUGIN_REGISTRY_DIR"
fi

cleanup() {
  docker_e2e_docker_cmd rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  rm -f "$RUN_LOG"
}
trap cleanup EXIT

docker_e2e_build_or_reuse "$IMAGE_NAME" lobster-loop-taskflow
OPENCLAW_TEST_STATE_SCRIPT_B64="$(docker_e2e_test_state_shell_b64 lobster-loop-taskflow empty)"

echo "Running packaged /loop -> Automation -> Lobster -> TaskFlow Docker E2E..."
if ! docker_e2e_run_with_harness \
  --name "$CONTAINER_NAME" \
  -e "PORT=$PORT" \
  -e "MOCK_PORT=$MOCK_PORT" \
  -e "CLICKCLACK_PORT=$CLICKCLACK_PORT" \
  -e "OPENCLAW_GATEWAY_TOKEN=$TOKEN" \
  -e "OPENAI_API_KEY=sk-openclaw-lobster-loop-taskflow-e2e" \
  -e "CLICKCLACK_BOT_TOKEN=clickclack-lobster-loop-taskflow-e2e" \
  -e "OPENCLAW_SKIP_GMAIL_WATCHER=1" \
  -e "OPENCLAW_SKIP_CANVAS_HOST=1" \
  -e "OPENCLAW_SKIP_ACPX_RUNTIME=1" \
  -e "OPENCLAW_SKIP_ACPX_RUNTIME_PROBE=1" \
  -e "OPENCLAW_TEST_STATE_SCRIPT_B64=$OPENCLAW_TEST_STATE_SCRIPT_B64" \
  ${OPENCLAW_PREPUBLISH_PLUGIN_REGISTRY_DOCKER_ARGS[@]+"${OPENCLAW_PREPUBLISH_PLUGIN_REGISTRY_DOCKER_ARGS[@]}"} \
  "$IMAGE_NAME" \
  bash -lc 'set -Eeuo pipefail
    source scripts/lib/openclaw-e2e-instance.sh
    source scripts/e2e/lib/prepublish-plugin-registry.sh
    openclaw_e2e_eval_test_state_from_b64 "${OPENCLAW_TEST_STATE_SCRIPT_B64:?missing OPENCLAW_TEST_STATE_SCRIPT_B64}"
    entry="$(openclaw_e2e_resolve_entrypoint)"
    scenario_tmp="$(mktemp -d "${TMPDIR:-/tmp}/openclaw-lobster-loop-taskflow.XXXXXX")"
    response_control="$scenario_tmp/mock-response-control.json"
    request_log="$scenario_tmp/mock-openai-requests.jsonl"
    clickclack_state="$scenario_tmp/clickclack.json"
    clickclack_log="$scenario_tmp/clickclack.log"
    gateway_log="$scenario_tmp/gateway.log"
    mock_log="$scenario_tmp/mock-openai.log"
    jobs_json="$scenario_tmp/jobs.json"
    run_json="$scenario_tmp/run.json"
    flows_json="$scenario_tmp/flows.json"
    flow_json="$scenario_tmp/flow.json"
    tasks_json="$scenario_tmp/tasks.json"
    mock_pid=""
    clickclack_pid=""
    gateway_pid=""
    plugin_registry_pid=""

    cleanup_inner() {
      openclaw_e2e_stop_process "${gateway_pid:-}"
      openclaw_e2e_stop_process "${clickclack_pid:-}"
      openclaw_e2e_stop_process "${mock_pid:-}"
      openclaw_e2e_stop_process "${plugin_registry_pid:-}"
      rm -rf "$scenario_tmp"
    }
    dump_logs() {
      local status="$1"
      if [ "$status" -ne 0 ]; then
        openclaw_e2e_dump_logs \
          "$gateway_log" \
          "$mock_log" \
          "$clickclack_log" \
          "$clickclack_state" \
          "$scenario_tmp/lobster-install.log" \
          "$scenario_tmp/clickclack-install.log" \
          "$scenario_tmp/plugin-registry/server.log" \
          "$request_log" \
          "$jobs_json" \
          "$run_json" \
          "$flows_json" \
          "$flow_json" \
          "$tasks_json"
      fi
    }
    trap cleanup_inner EXIT
    trap '\''status=$?; dump_logs "$status"; exit "$status"'\'' ERR

    node scripts/e2e/lib/lobster-loop-taskflow/write-fixture.mjs \
      "$OPENCLAW_CONFIG_PATH" "$response_control" "$MOCK_PORT" "$CLICKCLACK_PORT"
    openclaw_prepublish_plugin_registry_start_mounted \
      "$scenario_tmp/plugin-registry" plugin_registry_pid "[\"@openclaw/lobster\"]"
    lobster_install_args=("@openclaw/lobster")
    if [ -n "${OPENCLAW_PREPUBLISH_PLUGIN_REGISTRY_DIR:-}" ]; then
      lobster_install_args=("npm:@openclaw/lobster@${OPENCLAW_PREPUBLISH_PLUGIN_REGISTRY_CANDIDATE_VERSION:?missing candidate version}" --pin)
    fi
    openclaw_e2e_fixture_plugin_command openclaw -- \
      plugins install "${lobster_install_args[@]}" >"$scenario_tmp/lobster-install.log" 2>&1
    clickclack_plugin_dir="$scenario_tmp/clickclack-plugin"
    node scripts/e2e/lib/release-user-journey/write-clickclack-plugin.mjs \
      "$clickclack_plugin_dir"
    openclaw_e2e_fixture_plugin_command openclaw -- \
      plugins install "$clickclack_plugin_dir" --force \
      >"$scenario_tmp/clickclack-install.log" 2>&1
    export MOCK_RESPONSE_CONTROL="$response_control"
    export MOCK_REQUEST_LOG="$request_log"
    mock_pid="$(openclaw_e2e_start_mock_openai "$MOCK_PORT" "$mock_log")"
    openclaw_e2e_wait_mock_openai "$MOCK_PORT"
    CLICKCLACK_FIXTURE_PORT="$CLICKCLACK_PORT" \
    CLICKCLACK_FIXTURE_TOKEN="$CLICKCLACK_BOT_TOKEN" \
    CLICKCLACK_FIXTURE_STATE="$clickclack_state" \
      node scripts/e2e/lib/release-user-journey/clickclack-fixture.mjs \
      >"$clickclack_log" 2>&1 &
    clickclack_pid="$!"
    for _ in $(seq 1 100); do
      if openclaw_e2e_probe_http_status \
        "http://127.0.0.1:$CLICKCLACK_PORT/health" 200 >/dev/null 2>&1; then
        break
      fi
      sleep 0.1
    done
    openclaw_e2e_probe_http_status "http://127.0.0.1:$CLICKCLACK_PORT/health" 200

    gateway_pid="$(openclaw_e2e_start_gateway "$entry" "$PORT" "$gateway_log")"
    openclaw_e2e_wait_gateway_ready "$gateway_pid" "$gateway_log" 300 "$PORT"
    node scripts/e2e/lib/release-user-journey/assertions.mjs \
      wait-clickclack-socket "http://127.0.0.1:$CLICKCLACK_PORT" 45
    node scripts/e2e/lib/release-user-journey/assertions.mjs \
      post-clickclack-inbound "http://127.0.0.1:$CLICKCLACK_PORT" \
      "/loop 30s Run the Lobster commands.list pipeline as a managed TaskFlow and preserve the loop trace key in flow state."
    node scripts/e2e/lib/release-user-journey/assertions.mjs \
      wait-clickclack-reply "$clickclack_state" \
      "Loop created for the managed Lobster workflow." 45

    openclaw automations list --json >"$jobs_json"
    job_id="$(node scripts/e2e/lib/lobster-loop-taskflow/assertions.mjs job-id "$jobs_json")"
    openclaw automations run "$job_id" \
      --wait \
      --wait-timeout 2m \
      --poll-interval 500ms \
      --json >"$run_json"

    flow_id=""
    for _ in $(seq 1 120); do
      openclaw tasks flow list --json >"$flows_json"
      if flow_id="$(node scripts/e2e/lib/lobster-loop-taskflow/assertions.mjs flow-id-if-terminal "$flows_json")"; then
        break
      else
        status=$?
      fi
      if [ "$status" -ne 2 ]; then
        exit "$status"
      fi
      sleep 0.25
    done
    if [ -z "$flow_id" ]; then
      dump_logs 1
      echo "managed Lobster TaskFlow did not reach succeeded" >&2
      exit 1
    fi

    openclaw tasks flow show "$flow_id" --json >"$flow_json"
    openclaw tasks list --json >"$tasks_json"
    node scripts/e2e/lib/lobster-loop-taskflow/assertions.mjs verify \
      "$jobs_json" "$run_json" "$flow_json" "$tasks_json" "$request_log" "$clickclack_state"
  ' >"$RUN_LOG" 2>&1; then
  docker_e2e_print_log "$RUN_LOG"
  exit 1
fi

docker_e2e_print_log "$RUN_LOG"
echo "OK"
