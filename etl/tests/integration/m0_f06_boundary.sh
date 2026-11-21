#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
ETL="$ROOT/etl"
PYTHON="${BRACC_TEST_PYTHON:-python3}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$ROOT/.hermes-run/reports/f06-boundary}"
DEPS_DIR="${BRACC_TEST_DEPS_DIR:-}"
SCRATCH_BASE="${TMPDIR:-${XDG_CACHE_HOME:-$HOME/.cache}/bracc-f06-boundary}"
mkdir -p "$EVIDENCE_ROOT" "$SCRATCH_BASE"
EVIDENCE="$(mktemp -d "$EVIDENCE_ROOT/run-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXXXX")"
chmod 700 "$EVIDENCE"
LOGS="$EVIDENCE/logs"
mkdir "$LOGS"
RUN_ID="bracc-f06-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
NAME="$RUN_ID-neo4j"
VOLUME="$RUN_ID-data"
PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
IMAGE="neo4j:5.26.21-community"
IMAGE_ID="sha256:5cce41f4e2a2f69ac65b91769fd4e985668a8fe86e0e8c187830cc4de37722e9"
CONTAINER_ID=""
VOLUME_ID=""
ANON_VOLUMES=()
CONTAINER_OWNED=0
VOLUME_OWNED=0
CLEANUP_FAILED=0
TEST_RC=0

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
ETL_ROOT="$ETL" "$PYTHON" - <<'PY' >"$LOGS/preflight-python.txt" 2>&1 || fail "Python dependencies/import paths failed; see $LOGS/preflight-python.txt"
import hashlib
import json
import os
import sys
from pathlib import Path

import pytest
import neo4j
import httpx
import bracc_etl
from bracc_etl import base, extraction, loader
from bracc_etl.acquisition import extraction_bridge

root = Path(os.environ["ETL_ROOT"]).resolve()
src = (root / "src").resolve()
critical = {
    "bracc_etl": bracc_etl,
    "base": base,
    "extraction": extraction,
    "loader": loader,
    "extraction_bridge": extraction_bridge,
}
for name, module in critical.items():
    module_path = Path(module.__file__).resolve()
    assert module_path.is_relative_to(src), f"{name} imported outside selected worktree: {module_path}"
print("python", sys.version)
print("pytest", pytest.__version__)
print("neo4j", neo4j.__version__)
print("httpx", httpx.__version__)
print("critical_imports", json.dumps({name: str(Path(module.__file__).resolve()) for name, module in critical.items()}, sort_keys=True))
paths = ["tests/test_m0_f06_boundary.py", "tests/integration/m0_f06_boundary.sh", "../docs/unification/m0-f06-real-boundary-20261119.md"]
bindings = {rel: hashlib.sha256((root / rel).resolve().read_bytes()).hexdigest() for rel in paths}
print("selected_files_sha256", json.dumps(bindings, sort_keys=True))
PY
git -C "$ROOT" rev-parse HEAD >"$LOGS/head.txt"
git -C "$ROOT" status --short >"$LOGS/git-status-before.txt"
docker image inspect "$IMAGE" --format '{{.Id}}' >"$LOGS/image-id.txt" 2>"$LOGS/image-inspect.stderr" || fail "cached Neo4j image unavailable"
[[ "$(<"$LOGS/image-id.txt")" == "$IMAGE_ID" ]] || fail "Neo4j cached image digest mismatch"
if docker ps -a --no-trunc --filter "name=^/${NAME}$" --format '{{.ID}}' >"$LOGS/container-name-preflight.txt" 2>"$LOGS/container-name-preflight.stderr"; then
  [[ ! -s "$LOGS/container-name-preflight.txt" ]] || fail "generated container name already exists"
else fail "Docker container-name preflight query failed"; fi
if docker volume ls --filter "name=^${VOLUME}$" --format '{{.Name}}' >"$LOGS/volume-name-preflight.txt" 2>"$LOGS/volume-name-preflight.stderr"; then
  [[ ! -s "$LOGS/volume-name-preflight.txt" ]] || fail "generated volume name already exists"
else fail "Docker volume-name preflight query failed"; fi

cleanup() {
  local original_rc=$? inspect labels mounts rc
  local names_checked=""
  trap - EXIT INT TERM
  set +e
  if (( CONTAINER_OWNED )); then
    inspect="$(docker inspect "$CONTAINER_ID" 2>"$LOGS/container-cleanup-inspect.stderr")"; rc=$?
    if (( rc == 0 )); then
      labels="$(python3 -c 'import json,sys; x=json.load(sys.stdin)[0]; print(x["Config"]["Labels"].get("bracc.owner",""), x["Id"], sep="\n")' <<<"$inspect" 2>"$LOGS/container-cleanup-parse.stderr")"; rc=$?
      if (( rc == 0 )) && [[ "$labels" == "$RUN_ID
$CONTAINER_ID" ]]; then
        docker rm -f -v "$CONTAINER_ID" >"$LOGS/container-cleanup.stdout" 2>"$LOGS/container-cleanup.stderr" || CLEANUP_FAILED=1
      else printf 'ownership readback mismatch\n' >"$LOGS/container-cleanup.stderr"; CLEANUP_FAILED=1; fi
    elif docker ps -a --no-trunc --filter "id=$CONTAINER_ID" --format '{{.ID}}' >"$LOGS/container-absence.txt" 2>"$LOGS/container-absence.stderr"; then
      [[ ! -s "$LOGS/container-absence.txt" ]] || CLEANUP_FAILED=1
    else CLEANUP_FAILED=1; fi
  fi
  if (( VOLUME_OWNED )); then
    inspect="$(docker volume inspect "$VOLUME_ID" 2>"$LOGS/volume-cleanup-inspect.stderr")"; rc=$?
    if (( rc == 0 )); then
      labels="$(python3 -c 'import json,sys; x=json.load(sys.stdin)[0]; print(x["Labels"].get("bracc.owner",""), x["Name"], sep="\n")' <<<"$inspect" 2>"$LOGS/volume-cleanup-parse.stderr")"; rc=$?
      if (( rc == 0 )) && [[ "$labels" == "$RUN_ID
$VOLUME" ]]; then
        docker volume rm "$VOLUME_ID" >"$LOGS/volume-cleanup.stdout" 2>"$LOGS/volume-cleanup.stderr" || CLEANUP_FAILED=1
      else printf 'ownership readback mismatch\n' >"$LOGS/volume-cleanup.stderr"; CLEANUP_FAILED=1; fi
    elif docker volume ls --filter "name=^${VOLUME}$" --format '{{.Name}}' >"$LOGS/volume-absence.txt" 2>"$LOGS/volume-absence.stderr"; then
      [[ ! -s "$LOGS/volume-absence.txt" ]] || CLEANUP_FAILED=1
    else CLEANUP_FAILED=1; fi
  fi
  if (( CONTAINER_OWNED )); then
    docker inspect "$CONTAINER_ID" >"$LOGS/container-after.json" 2>"$LOGS/container-after.stderr"; rc=$?
    if (( rc == 0 )); then CLEANUP_FAILED=1
    elif ! docker ps -a --no-trunc --filter "id=$CONTAINER_ID" --format '{{.ID}}' >"$LOGS/container-after.txt" 2>"$LOGS/container-after-query.stderr" || [[ -s "$LOGS/container-after.txt" ]]; then CLEANUP_FAILED=1; fi
  fi
  for anonymous_volume in "${ANON_VOLUMES[@]}"; do
    docker volume inspect "$anonymous_volume" >"$LOGS/anonymous-volume-after-${anonymous_volume}.json" 2>"$LOGS/anonymous-volume-after-${anonymous_volume}.stderr"; rc=$?
    if (( rc == 0 )); then CLEANUP_FAILED=1
    elif ! docker volume ls --filter "name=^${anonymous_volume}$" --format '{{.Name}}' >"$LOGS/anonymous-volume-after-${anonymous_volume}.txt" 2>"$LOGS/anonymous-volume-after-${anonymous_volume}-query.stderr" || [[ -s "$LOGS/anonymous-volume-after-${anonymous_volume}.txt" ]]; then CLEANUP_FAILED=1; fi
  done
  if (( VOLUME_OWNED )); then
    docker volume inspect "$VOLUME_ID" >"$LOGS/volume-after.json" 2>"$LOGS/volume-after.stderr"; rc=$?
    if (( rc == 0 )); then CLEANUP_FAILED=1
    elif ! docker volume ls --filter "name=^${VOLUME}$" --format '{{.Name}}' >"$LOGS/volume-after.txt" 2>"$LOGS/volume-after-query.stderr" || [[ -s "$LOGS/volume-after.txt" ]]; then CLEANUP_FAILED=1; fi
  fi
  printf '{"test_exit":%s,"cleanup_failed":%s,"container_owned":%s,"volume_owned":%s}\n' "$original_rc" "$CLEANUP_FAILED" "$CONTAINER_OWNED" "$VOLUME_OWNED" >"$LOGS/cleanup.json"
  if (( CLEANUP_FAILED )); then exit 1; fi
  exit "$original_rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$RUN_ID" >"$LOGS/run-id.txt"

docker volume create --label "bracc.owner=$RUN_ID" "$VOLUME" >"$LOGS/volume-create.stdout" 2>"$LOGS/volume-create.stderr" || fail "volume creation failed"
VOLUME_ID="$(<"$LOGS/volume-create.stdout")"
if docker volume inspect "$VOLUME_ID" >"$LOGS/volume-created-inspect.json" 2>"$LOGS/volume-created-inspect.stderr" && python3 - "$RUN_ID" "$VOLUME" "$VOLUME_ID" "$LOGS/volume-created-inspect.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[4]))[0]
assert x["Name"]==sys.argv[2] and x["Name"]==sys.argv[3]
assert x["Labels"].get("bracc.owner")==sys.argv[1]
PY
then VOLUME_OWNED=1; else fail "created volume ownership could not be verified"; fi

docker run -d --name "$NAME" --label "bracc.owner=$RUN_ID" \
  -p 127.0.0.1::7687 -e "NEO4J_AUTH=neo4j/$PASSWORD" -v "$VOLUME:/data" \
  --log-driver none "$IMAGE" >"$LOGS/container-create.stdout" 2>"$LOGS/container-create.stderr" || fail "container creation failed"
CONTAINER_ID="$(<"$LOGS/container-create.stdout")"
if docker inspect "$CONTAINER_ID" >"$LOGS/container-created-inspect.json" 2>"$LOGS/container-created-inspect.stderr" && python3 - "$RUN_ID" "$NAME" "$CONTAINER_ID" "$VOLUME" "$LOGS/container-created-inspect.json" "$LOGS/anonymous-volumes-owned.txt" <<'PY'
import json,sys
x=json.load(open(sys.argv[5]))[0]
assert x["Id"]==sys.argv[3] and x["Name"]=="/"+sys.argv[2]
assert x["Config"]["Labels"].get("bracc.owner")==sys.argv[1]
mounts=x["Mounts"]
data=[m for m in mounts if m["Destination"]=="/data"]
logs=[m for m in mounts if m["Destination"]=="/logs"]
assert len(data)==1 and data[0]["Type"]=="volume" and data[0]["Name"]==sys.argv[4]
assert len(logs)==1 and logs[0]["Type"]=="volume" and len(logs[0]["Name"])==64
assert all(m["Type"]=="volume" and m["Destination"] in {"/data","/logs"} for m in mounts)
open(sys.argv[6],"w").write(logs[0]["Name"])
PY
then
  CONTAINER_OWNED=1
  mapfile -t ANON_VOLUMES <"$LOGS/anonymous-volumes-owned.txt"
else fail "created container ownership/mounts could not be verified"; fi
PORT="$(docker port "$CONTAINER_ID" 7687/tcp 2>"$LOGS/port.stderr" | sed 's/.*://')" || fail "ephemeral port lookup failed"
[[ "$PORT" =~ ^[0-9]+$ ]] || fail "invalid ephemeral port"
URI="bolt://127.0.0.1:$PORT"
printf '%s\n' "$URI" >"$LOGS/neo4j-uri.txt"
READY=0
for _ in $(seq 1 "${BRACC_NEO4J_READY_TIMEOUT:-90}"); do
  if BRACC_TEST_NEO4J_PASSWORD="$PASSWORD" "$PYTHON" -c 'import os,sys; from neo4j import GraphDatabase; d=GraphDatabase.driver(sys.argv[1],auth=("neo4j",os.environ["BRACC_TEST_NEO4J_PASSWORD"])); d.verify_connectivity(); d.close()' "$URI" >"$LOGS/neo4j-readiness.stdout" 2>"$LOGS/neo4j-readiness.stderr"; then READY=1; break; fi
  sleep 1
done
(( READY == 1 )) || { printf 'Neo4j readiness timeout\n' >"$LOGS/readiness-failure.txt"; fail "Neo4j not ready before timeout"; }
export BRACC_TEST_NEO4J_URI="$URI" BRACC_TEST_NEO4J_PASSWORD="$PASSWORD" TMPDIR="$SCRATCH_BASE"
cd "$ETL"
set +e
"$PYTHON" -m pytest -c "$ETL/pyproject.toml" -o addopts='' -m integration -vv --junitxml="$LOGS/junit.xml" "$ETL/tests/test_m0_f06_boundary.py" >"$LOGS/pytest.stdout" 2>"$LOGS/pytest.stderr"
TEST_RC=$?
set -e
printf '%s\n' "$TEST_RC" >"$LOGS/pytest.exit"
exit "$TEST_RC"
