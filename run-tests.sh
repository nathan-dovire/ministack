#!/usr/bin/env bash
# Run the ministack test suite in a disposable, isolated container, leaving the
# running platform ministack (host port 4566) and its data untouched. The
# server and pytest run together in one throwaway container with:
#   * a fresh STATE_DIR per run, shared by server and test client (so the RSA
#     signing key matches and the JWT-authorizer tests pass),
#   * no published ports (the in-container ministack is not reachable from the
#     host stack),
#   * the host Docker socket mounted, so tests that spin sibling containers
#     (rds / ecs / eks / elasticache / batch) work,
#   * a Linux runtime, so results match the published image rather than the
#     host OS.
#
# The image carries only dependencies; the ministack source is bind-mounted
# live, so edits are picked up without a rebuild.
#
# Usage:
#   ./run-tests.sh                              # whole suite (serial)
#   ./run-tests.sh tests/test_cognito.py        # a file
#   ./run-tests.sh tests/test_cognito.py -k oidc -x   # any pytest args
#   REBUILD=1 ./run-tests.sh                     # force-rebuild the test image
#   CA_BUNDLE=/path/ca.pem REBUILD=1 ./run-tests.sh   # build with an extra CA bundle
#   LAMBDA_EXECUTOR=docker ./run-tests.sh        # exercise the docker Lambda executor
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE="ministack-test:local"

# Build the deps image once; rebuild only on demand or when missing.
if [[ -n "${REBUILD:-}" ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "==> building $IMAGE (deps only; source is mounted live at run time)"
    # Pass an extra CA bundle (env CA_BUNDLE, a file path) as an optional build
    # secret (not a COPY) so a missing cert never breaks the build.
    build_secret=()
    if [[ -n "${CA_BUNDLE:-}" && -f "$CA_BUNDLE" ]]; then
        build_secret=(--secret "id=cabundle,src=$CA_BUNDLE")
    fi
    DOCKER_BUILDKIT=1 docker build "${build_secret[@]}" -f Dockerfile.test -t "$IMAGE" .
fi

# Default to the whole suite; otherwise forward whatever the caller passed.
PYTEST_ARGS=("$@")
[[ ${#PYTEST_ARGS[@]} -eq 0 ]] && PYTEST_ARGS=(tests/)

# Forward LAMBDA_EXECUTOR only if the caller set it. Do NOT inject AWS_* creds:
# the local Lambda executor runs in-process, so they would leak into invoked
# functions and break the account-context tests. conftest supplies per-client
# creds.
RUN_ENV=()
[[ -n "${LAMBDA_EXECUTOR:-}" ]] && RUN_ENV+=(-e "LAMBDA_EXECUTOR=$LAMBDA_EXECUTOR")

# Tests spin sibling containers on the host daemon (mwaa/rds/ecs/eks/
# elasticache/opensearch) that `docker run --rm` won't remove. ministack labels
# every container it spawns `label=ministack`, so reap only the ones THIS run
# created by diffing against a pre-run snapshot, leaving the platform's own
# spawned containers untouched. Runs on EXIT (success, failure, or Ctrl-C).
_snapshot() { docker ps -aq --filter label=ministack 2>/dev/null | sort -u; }
PRE_SPAWNED="$(_snapshot)"
reap_spawned() {
    local fresh
    fresh="$(comm -13 <(printf '%s\n' "$PRE_SPAWNED") <(_snapshot) | grep -v '^$' || true)"
    if [[ -n "$fresh" ]]; then
        echo "==> reaping $(printf '%s\n' "$fresh" | grep -c .) ministack-spawned sibling container(s) from this run"
        printf '%s\n' "$fresh" | xargs -r docker rm -f >/dev/null 2>&1 || true
    fi
}
trap reap_spawned EXIT

echo "==> running ministack tests in an isolated container (platform ministack untouched)"
docker run --rm -t \
    -v "$SCRIPT_DIR:/work" \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e PYTHONPATH=/work \
    -e STATE_DIR=/tmp/ministack-test-state \
    -e MINISTACK_ENDPOINT=http://localhost:4566 \
    -e GATEWAY_PORT=4566 \
    -e LOG_LEVEL=WARNING \
    "${RUN_ENV[@]}" \
    -w /work \
    "$IMAGE" \
    bash -c '
        set -e
        # Start the isolated ministack inside this container.
        python -m ministack >/tmp/ministack-server.log 2>&1 &
        server_pid=$!
        # Wait for it to answer (urllib, so we depend on no extra binaries).
        ready=0
        for _ in $(seq 1 60); do
            if python - <<"PY" 2>/dev/null
import urllib.request, sys
sys.exit(0 if urllib.request.urlopen("http://localhost:4566/_ministack/health", timeout=2).status == 200 else 1)
PY
            then ready=1; break; fi
            sleep 1
        done
        if [ "$ready" != "1" ]; then
            echo "ministack did not become healthy; server log:" >&2
            tail -50 /tmp/ministack-server.log >&2
            exit 1
        fi
        # Serial run: one shared in-container server, so parallel workers would
        # collide on global state. `addopts=""` drops the repo default
        # --dist=loadfile.
        #
        # Deselect test_lambda_docker_flags_applied_to_run_kwargs: over the
        # mounted socket ministack takes the containers.create()+docker-cp path
        # (bind-mounts cannot cross the socket), which the test mock of
        # containers.run() does not cover. It runs when the daemon is local
        # (host/CI).
        # `|| rc=$?` so a non-zero pytest does not trip `set -e` before the
        # explicit server cleanup below runs.
        rc=0
        python -m pytest -o addopts="" -n0 -p no:cacheprovider -rf \
            --deselect "tests/test_lambda.py::test_lambda_docker_flags_applied_to_run_kwargs" \
            "$@" || rc=$?
        kill "$server_pid" 2>/dev/null || true
        exit $rc
    ' _ "${PYTEST_ARGS[@]}"
