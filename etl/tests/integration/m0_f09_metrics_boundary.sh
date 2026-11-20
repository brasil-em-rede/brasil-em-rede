#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
ETL="$ROOT/etl"
PYTHON="${BRACC_TEST_PYTHON:-python3}"
DEPS_DIR="${BRACC_TEST_DEPS_DIR:-}"
SCRATCH_BASE="${TMPDIR:?set TMPDIR to an absolute writable scratch directory}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:?set EVIDENCE_ROOT to an exclusive absolute directory}"
IMAGE="neo4j:5.26.21-community"
IMAGE_ID="sha256:5cce41f4e2a2f69ac65b91769fd4e985668a8fe86e0e8c187830cc4de37722e9"
mkdir -p "$EVIDENCE_ROOT" "$SCRATCH_BASE"
EVIDENCE="$(mktemp -d "$EVIDENCE_ROOT/run-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXXXX")"
chmod 700 "$EVIDENCE"
LOGS="$EVIDENCE/logs"
mkdir "$LOGS"
RUN_ID="bracc-f09-metrics-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
NAME="$RUN_ID-neo4j"
VOLUME="$RUN_ID-data"
PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
CONTAINER_ID=""
VOLUME_ID=""
CONTAINER_OWNED=0
VOLUME_OWNED=0
CLEANUP_FAILED=0

fail() { printf 'preflight: %s\n' "$*" >&2; exit 2; }
command -v docker >/dev/null || fail "docker is required"
command -v "$PYTHON" >/dev/null || fail "configured Python not found: $PYTHON"
[[ -z "$DEPS_DIR" || -d "$DEPS_DIR" ]] || fail "dependency directory does not exist: $DEPS_DIR"
if [[ -n "$DEPS_DIR" ]]; then
  export PYTHONPATH="$ETL/src:$DEPS_DIR${PYTHONPATH:+:$PYTHONPATH}"
else
  export PYTHONPATH="$ETL/src${PYTHONPATH:+:$PYTHONPATH}"
fi
export PYTHONDONTWRITEBYTECODE=1
ETL_ROOT="$ETL" "$PYTHON" - <<'PY' >"$LOGS/preflight-python.txt" 2>&1 || fail "selected runtime/import binding failed"
import hashlib
import os
import sys
from pathlib import Path
import neo4j
import pandas
import pytest
import bracc_etl
from bracc_etl.dimensions import projection
from bracc_etl import loader

root = Path(os.environ["ETL_ROOT"]).resolve()
src = (root / "src").resolve()
for name, module in {"bracc_etl": bracc_etl, "projection": projection, "loader": loader}.items():
    path = Path(module.__file__).resolve()
    assert path.is_relative_to(src), f"{name} imported outside selected worktree: {path}"
    print(name, path)
print("projection_function_code_filename", projection.project_rais.__code__.co_filename)
print("python", sys.executable, sys.version)
print("pytest", pytest.__version__)
print("neo4j", neo4j.__version__)
print("pandas", pandas.__version__)
for relative in (
    "tests/dimensions/test_f09_metrics_boundary.py",
    "tests/integration/m0_f09_metrics_boundary.sh",
    "../docs/unification/m0-f09-metrics-boundary-20261119.md",
):
    path = root / relative
    print(relative, hashlib.sha256(path.read_bytes()).hexdigest())
PY

git -C "$ROOT" rev-parse HEAD >"$LOGS/head.txt"
git -C "$ROOT" status --short >"$LOGS/git-status-before.txt"
docker image inspect "$IMAGE" --format '{{.Id}}' >"$LOGS/image-id.txt" 2>"$LOGS/image-inspect.stderr" || fail "cached Neo4j image unavailable"
[[ "$(<"$LOGS/image-id.txt")" == "$IMAGE_ID" ]] || fail "cached Neo4j image digest mismatch"
docker ps -a --no-trunc --filter "name=^/${NAME}$" --format '{{.ID}}' >"$LOGS/container-preflight.txt" 2>"$LOGS/container-preflight.stderr" || fail "container-name preflight failed"
[[ ! -s "$LOGS/container-preflight.txt" ]] || fail "generated container name already exists"
docker volume ls --filter "name=^${VOLUME}$" --format '{{.Name}}' >"$LOGS/volume-preflight.txt" 2>"$LOGS/volume-preflight.stderr" || fail "volume-name preflight failed"
[[ ! -s "$LOGS/volume-preflight.txt" ]] || fail "generated volume name already exists"

cleanup() {
  local original_rc=$? inspect labels check
  trap - EXIT INT TERM
  set +e
  if (( CONTAINER_OWNED )); then
    if docker inspect "$CONTAINER_ID" >"$LOGS/container-cleanup-inspect.json" 2>"$LOGS/container-cleanup-inspect.stderr"; then
      labels="$(python3 -c 'import json,sys; x=json.load(sys.stdin)[0]; print(x["Config"]["Labels"].get("bracc.owner",""),x["Id"],sep="\n")' <"$LOGS/container-cleanup-inspect.json" 2>"$LOGS/container-cleanup-parse.stderr")"
      if [[ "$labels" == "$RUN_ID
$CONTAINER_ID" ]]; then
        docker rm -f -v "$CONTAINER_ID" >"$LOGS/container-cleanup.stdout" 2>"$LOGS/container-cleanup.stderr" || CLEANUP_FAILED=1
      else
        printf 'container ownership readback mismatch\n' >"$LOGS/container-cleanup.stderr"
        CLEANUP_FAILED=1
      fi
    else
      CLEANUP_FAILED=1
    fi
  fi
  if (( VOLUME_OWNED )); then
    if docker volume inspect "$VOLUME_ID" >"$LOGS/volume-cleanup-inspect.json" 2>"$LOGS/volume-cleanup-inspect.stderr"; then
      labels="$(python3 -c 'import json,sys; x=json.load(sys.stdin)[0]; print(x["Labels"].get("bracc.owner",""),x["Name"],sep="\n")' <"$LOGS/volume-cleanup-inspect.json" 2>"$LOGS/volume-cleanup-parse.stderr")"
      if [[ "$labels" == "$RUN_ID
$VOLUME" ]]; then
        docker volume rm "$VOLUME_ID" >"$LOGS/volume-cleanup.stdout" 2>"$LOGS/volume-cleanup.stderr" || CLEANUP_FAILED=1
      else
        printf 'volume ownership readback mismatch\n' >"$LOGS/volume-cleanup.stderr"
        CLEANUP_FAILED=1
      fi
    else
      CLEANUP_FAILED=1
    fi
  fi
  if (( CONTAINER_OWNED )); then
    docker inspect "$CONTAINER_ID" >"$LOGS/container-after.json" 2>"$LOGS/container-after.stderr"
    check=$?
    if (( check == 0 )); then
      CLEANUP_FAILED=1
    elif ! docker ps -a --no-trunc --filter "id=$CONTAINER_ID" --format '{{.ID}}' >"$LOGS/container-after.txt" 2>"$LOGS/container-after-query.stderr" || [[ -s "$LOGS/container-after.txt" ]]; then
      CLEANUP_FAILED=1
    fi
  fi
  if (( VOLUME_OWNED )); then
    docker volume inspect "$VOLUME_ID" >"$LOGS/volume-after.json" 2>"$LOGS/volume-after.stderr"
    check=$?
    if (( check == 0 )); then
      CLEANUP_FAILED=1
    elif ! docker volume ls --filter "name=^${VOLUME}$" --format '{{.Name}}' >"$LOGS/volume-after.txt" 2>"$LOGS/volume-after-query.stderr" || [[ -s "$LOGS/volume-after.txt" ]]; then
      CLEANUP_FAILED=1
    fi
  fi
  printf '{"test_exit":%s,"cleanup_failed":%s,"container_owned":%s,"volume_owned":%s}\n' \
    "$original_rc" "$CLEANUP_FAILED" "$CONTAINER_OWNED" "$VOLUME_OWNED" >"$LOGS/cleanup.json"
  if (( CLEANUP_FAILED )); then exit 1; fi
  exit "$original_rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$RUN_ID" >"$LOGS/run-id.txt"

docker volume create --label "bracc.owner=$RUN_ID" "$VOLUME" >"$LOGS/volume-create.stdout" 2>"$LOGS/volume-create.stderr" || fail "volume creation failed"
VOLUME_ID="$(<"$LOGS/volume-create.stdout")"
if docker volume inspect "$VOLUME_ID" >"$LOGS/volume-created-inspect.json" 2>"$LOGS/volume-created-inspect.stderr" && \
   python3 - "$RUN_ID" "$VOLUME" "$VOLUME_ID" "$LOGS/volume-created-inspect.json" <<'PY'
import json
import sys
from pathlib import Path

record = json.loads(Path(sys.argv[4]).read_text())[0]
assert record["Name"] == sys.argv[2] == sys.argv[3]
assert record["Labels"].get("bracc.owner") == sys.argv[1]
PY
then
  VOLUME_OWNED=1
else
  fail "created volume ownership could not be verified"
fi

docker run -d --name "$NAME" --label "bracc.owner=$RUN_ID" \
  -p 127.0.0.1::7687 -e "NEO4J_AUTH=neo4j/$PASSWORD" \
  --mount "type=volume,source=$VOLUME,target=/data" --tmpfs /logs \
  --log-driver none "$IMAGE" >"$LOGS/container-create.stdout" 2>"$LOGS/container-create.stderr" || fail "container creation failed"
CONTAINER_ID="$(<"$LOGS/container-create.stdout")"
CONTAINER_OWNED=1
if docker inspect "$CONTAINER_ID" >"$LOGS/container-created-inspect.json" 2>"$LOGS/container-created-inspect.stderr" && \
   python3 - "$RUN_ID" "$NAME" "$CONTAINER_ID" "$VOLUME" "$LOGS/container-created-inspect.json" <<'PY'
import json
import sys
from pathlib import Path

record = json.loads(Path(sys.argv[5]).read_text())[0]
assert record["Id"] == sys.argv[3] and record["Name"] == "/" + sys.argv[2]
assert record["Config"]["Labels"].get("bracc.owner") == sys.argv[1]
mounts = record["Mounts"]
data = [mount for mount in mounts if mount["Destination"] == "/data"]
assert len(data) == 1 and data[0]["Type"] == "volume" and data[0]["Name"] == sys.argv[4]
assert record["HostConfig"]["Tmpfs"].get("/logs") is not None
assert len(mounts) == 1
PY
then
  :
else
  fail "created container identity or complete mount inventory failed"
fi
PORT="$(docker port "$CONTAINER_ID" 7687/tcp 2>"$LOGS/port.stderr" | sed 's/.*://')" || fail "ephemeral port lookup failed"
[[ "$PORT" =~ ^[0-9]+$ ]] || fail "invalid ephemeral port"
URI="bolt://127.0.0.1:$PORT"
printf '%s\n' "$URI" >"$LOGS/neo4j-uri.txt"
READY=0
for _ in $(seq 1 "${BRACC_NEO4J_READY_TIMEOUT:-90}"); do
  if BRACC_TEST_NEO4J_PASSWORD="$PASSWORD" "$PYTHON" -c \
    'import os,sys; from neo4j import GraphDatabase; d=GraphDatabase.driver(sys.argv[1],auth=("neo4j",os.environ["BRACC_TEST_NEO4J_PASSWORD"])); d.verify_connectivity(); d.close()' \
    "$URI" >"$LOGS/neo4j-readiness.stdout" 2>"$LOGS/neo4j-readiness.stderr"; then
    READY=1
    break
  fi
  sleep 1
done
(( READY == 1 )) || fail "Neo4j not ready before timeout"
export BRACC_TEST_NEO4J_URI="$URI" BRACC_TEST_NEO4J_PASSWORD="$PASSWORD" \
  BRACC_TEST_NEO4J_DATABASE=neo4j TMPDIR="$SCRATCH_BASE"
cd "$ETL"
PYTEST_ARGS=(
  -m pytest -c "$ETL/pyproject.toml" -o addopts='' -m integration -vv
  --junitxml="$LOGS/junit.xml" "$ETL/tests/dimensions/test_f09_metrics_boundary.py"
)
"$PYTHON" - "$LOGS/pytest.argv.json" "$PYTHON" "${PYTEST_ARGS[@]}" <<'PY'
import json
import os
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(
    json.dumps(
        {"cwd": os.getcwd(), "interpreter": sys.argv[2], "argv": sys.argv[3:]},
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY
set +e
"$PYTHON" "${PYTEST_ARGS[@]}" >"$LOGS/pytest.stdout" 2>"$LOGS/pytest.stderr"
TEST_RC=$?
set -e
printf '%s\n' "$TEST_RC" >"$LOGS/pytest.exit"
exit "$TEST_RC"
