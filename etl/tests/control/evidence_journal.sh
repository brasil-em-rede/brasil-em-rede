#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
work=${BRACC_EVIDENCE_TMPDIR:-${TMPDIR:-}}
[[ -n "$work" ]] || { printf 'Set TMPDIR or BRACC_EVIDENCE_TMPDIR to an authorized scratch directory\n' >&2; exit 2; }
python_bin=${BRACC_TEST_PYTHON:-$root/etl/.venv/bin/python}
"$python_bin" -c 'import psycopg' >/dev/null
command -v docker >/dev/null
attempt=${BRACC_EVIDENCE_ATTEMPT_ID:-"${$}-${RANDOM}"}
name="bracc-u05-journal-${attempt}"
image="${name}:test"
docker_error_is_not_found() {
  local kind="$1" target="${2,,}" message="${3,,}"
  case "$kind" in
    container)
      [[ "$message" == *"no such container: $target"* || "$message" == *"no such object: $target"* ]]
      ;;
    image)
      [[ "$message" == *"no such image: $target"* ]]
      ;;
    volume)
      [[ "$message" == *"no such volume"* && "$message" == *"$target"* ]]
      ;;
    *)
      return 1
      ;;
  esac
}
docker_preflight_inspect() {
  local kind="$1" target="$2" output rc
  if [[ "$kind" == container ]]; then
    if output=$(docker container inspect "$target" 2>&1); then return 0; else rc=$?; fi
  else
    if output=$(docker image inspect "$target" 2>&1); then return 0; else rc=$?; fi
  fi
  if [[ "$rc" -eq 1 ]] && docker_error_is_not_found "$kind" "$target" "$output"; then
    return 1
  fi
  printf 'Docker preflight inspection failed for %s %q (exit %s): %s\n' \
    "$kind" "$target" "$rc" "$output" >&2
  return 2
}
if docker_preflight_inspect container "$name"; then
  printf 'Refusing pre-existing Docker resource name/tag\n' >&2
  exit 1
else
  inspect_status=$?
  [[ "$inspect_status" -eq 1 ]] || exit 1
fi
if docker_preflight_inspect image "$image"; then
  printf 'Refusing pre-existing Docker resource name/tag\n' >&2
  exit 1
else
  inspect_status=$?
  [[ "$inspect_status" -eq 1 ]] || exit 1
fi
tmp=
container_id=
container_acquired=false
image_id=
image_acquired=false
docker_cleanup_absence() {
  local kind="$1" target="$2" output inspect_status
  case "$kind" in
    container)
      if output=$(docker container inspect "$target" 2>&1); then
        printf 'Container remains after cleanup: %s\n' "$target" >&2
        return 1
      else
        inspect_status=$?
      fi
      ;;
    image)
      if output=$(docker image inspect "$target" 2>&1); then
        printf 'Image tag remains after cleanup: %s\n' "$target" >&2
        return 1
      else
        inspect_status=$?
      fi
      ;;
    volume)
      if output=$(docker volume inspect "$target" 2>&1); then
        printf 'Volume remains after cleanup: %s\n' "$target" >&2
        return 1
      else
        inspect_status=$?
      fi
      ;;
    *)
      printf 'Cleanup cannot verify unsupported Docker resource kind: %s\n' "$kind" >&2
      return 1
      ;;
  esac
  if [[ "$inspect_status" -eq 1 ]] && docker_error_is_not_found "$kind" "$target" "$output"; then
    return 0
  fi
  printf 'Cleanup could not confirm %s absence for %q (exit %s): %s\n' \
    "$kind" "$target" "$inspect_status" "$output" >&2
  return 1
}
cleanup() {
  local primary_status=$? cleanup_status=0 inspect_status remove_status
  local current_container_info current_container_id current_container_name current_container_owner current_container_mounts
  local current_image_id image_tags image_owner image_info mounts_for_cleanup volume_names volume_id
  local -a volume_ids=()
  trap - EXIT
  mounts_for_cleanup=${container_mounts:-[]}
  if [[ "$container_acquired" == true ]]; then
    if current_container_info=$(docker container inspect --format '{{.Id}}|{{.Name}}|{{index .Config.Labels "bracc.owner"}}|{{json .Mounts}}' "$container_id" 2>&1); then
      IFS='|' read -r current_container_id current_container_name current_container_owner current_container_mounts <<< "$current_container_info"
      if [[ "$current_container_id" == "$container_id" && "$current_container_name" == "/$name" && "$current_container_owner" == "$name" ]]; then
        mounts_for_cleanup=$current_container_mounts
        if docker stop -t 10 "$container_id" >/dev/null; then
          :
        else
          remove_status=$?
          printf 'Cleanup could not stop owned container %s (exit %s)\n' "$container_id" "$remove_status" >&2
          cleanup_status=1
        fi
        if docker rm -fv "$container_id" >/dev/null; then
          :
        else
          remove_status=$?
          printf 'Cleanup could not remove owned container %s (exit %s)\n' "$container_id" "$remove_status" >&2
          cleanup_status=1
        fi
      else
        printf 'Cleanup refused container whose identity or owner changed: %s\n' "$container_id" >&2
        cleanup_status=1
      fi
    else
      inspect_status=$?
      if ! { [[ "$inspect_status" -eq 1 ]] && docker_error_is_not_found container "$container_id" "$current_container_info"; }; then
        printf 'Cleanup could not confirm owned container %s (exit %s): %s\n' \
          "$container_id" "$inspect_status" "$current_container_info" >&2
        cleanup_status=1
      fi
    fi
    if ! docker_cleanup_absence container "$container_id"; then
      cleanup_status=1
    fi
    if volume_names=$(printf '%s' "$mounts_for_cleanup" | "$python_bin" -c 'import json,sys; [print(item["Name"]) for item in json.loads(sys.stdin.read()) if item.get("Type") == "volume" and isinstance(item.get("Name"), str)]'); then
      if [[ -n "$volume_names" ]]; then
        mapfile -t volume_ids <<< "$volume_names"
      fi
    else
      inspect_status=$?
      printf 'Cleanup could not enumerate owned container volumes (exit %s)\n' "$inspect_status" >&2
      cleanup_status=1
    fi
    for volume_id in "${volume_ids[@]}"; do
      if [[ ! "$volume_id" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
        printf 'Cleanup refused invalid inventoried volume identifier: %q\n' "$volume_id" >&2
        cleanup_status=1
      elif ! docker_cleanup_absence volume "$volume_id"; then
        cleanup_status=1
      fi
    done
  fi
  if [[ "$image_acquired" == true ]]; then
    if image_info=$(docker image inspect --format '{{.Id}}|{{json .RepoTags}}|{{index .Config.Labels "bracc.owner"}}' "$image" 2>&1); then
      IFS='|' read -r current_image_id image_tags image_owner <<< "$image_info"
      if [[ "$current_image_id" == "$image_id" && "$image_tags" == *"\"$image\""* && "$image_owner" == "$name" ]]; then
        if docker image rm "$image" >/dev/null; then
          :
        else
          remove_status=$?
          printf 'Cleanup could not remove owned image tag %s (exit %s)\n' "$image" "$remove_status" >&2
          cleanup_status=1
        fi
      else
        printf 'Cleanup refused image tag whose ownership changed: %s\n' "$image" >&2
        cleanup_status=1
      fi
    else
      inspect_status=$?
      if ! { [[ "$inspect_status" -eq 1 ]] && docker_error_is_not_found image "$image" "$image_info"; }; then
        printf 'Cleanup could not confirm owned image tag %s (exit %s): %s\n' \
          "$image" "$inspect_status" "$image_info" >&2
        cleanup_status=1
      fi
    fi
    if ! docker_cleanup_absence image "$image"; then
      cleanup_status=1
    fi
  fi
  if [[ -n "$tmp" && -d "$tmp" ]]; then
    if rm -rf -- "$tmp"; then
      if [[ -e "$tmp" ]]; then
        printf 'Cleanup could not remove temporary directory: %s\n' "$tmp" >&2
        cleanup_status=1
      fi
    else
      remove_status=$?
      printf 'Cleanup could not remove temporary directory %s (exit %s)\n' "$tmp" "$remove_status" >&2
      cleanup_status=1
    fi
  fi
  if [[ "$primary_status" -ne 0 ]]; then
    if [[ "$cleanup_status" -ne 0 ]]; then
      printf 'Primary operation failed with exit %s; cleanup also failed with exit %s\n' \
        "$primary_status" "$cleanup_status" >&2
    fi
    return "$primary_status"
  fi
  return "$cleanup_status"
}
trap cleanup EXIT
mkdir -p "$work"
tmp=$(mktemp -d "${work%/}/u5.XXXXXX")
chmod 755 "$tmp"
port=$("$python_bin" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
docker build --label "bracc.owner=$name" -t "$image" "$root/infra/postgres" >/dev/null
if image_info=$(docker image inspect --format '{{.Id}}|{{json .RepoTags}}|{{index .Config.Labels "bracc.owner"}}' "$image" 2>&1); then
  :
else
  inspect_status=$?
  printf 'Built image identity/tag readback failed for %s (exit %s): %s\n' \
    "$image" "$inspect_status" "$image_info" >&2
  exit 1
fi
IFS='|' read -r image_id image_tags image_owner <<< "$image_info"
if [[ ! "$image_id" =~ ^sha256:[0-9a-f]{64}$ || "$image_tags" != *"\"$image\""* || "$image_owner" != "$name" ]]; then
  printf 'Built image identity/tag readback did not match this run: %s\n' "$image" >&2
  exit 1
fi
image_acquired=true
printf 'OWNED image id=%s tag=%s owner=%s\n' "$image_id" "$image" "$image_owner" >&2
container_id=$(docker run -d --name "$name" --label "bracc.owner=$name" --publish "127.0.0.1:${port}:5432" -e POSTGRES_PASSWORD=synthetic-only "$image")
if container_info=$(docker container inspect --format '{{.Id}}|{{.Name}}|{{index .Config.Labels "bracc.owner"}}|{{json .Mounts}}' "$container_id" 2>&1); then
  IFS='|' read -r inspected_container_id inspected_container_name container_owner container_mounts <<< "$container_info"
else
  inspect_status=$?
  printf 'New container identity/owner readback failed for %s (exit %s): %s\n' \
    "$container_id" "$inspect_status" "$container_info" >&2
  exit 1
fi
if [[ "$inspected_container_id" != "$container_id" || "$inspected_container_name" != "/$name" || "$container_owner" != "$name" ]]; then
  printf 'New container identity/owner readback did not match this run: %s\n' "$container_id" >&2
  exit 1
fi
container_acquired=true
printf 'OWNED container=%s name=%s image=%s port=127.0.0.1:%s tmp=%s\n' \
  "$container_id" "$name" "$image" "$port" "$tmp" >&2
printf 'MOUNTS %s\n' "$container_mounts" >&2
ready=false
for _ in {1..60}; do
  if docker logs "$name" 2>&1 | grep -Fq 'PostgreSQL init process complete; ready for start up.' && docker exec "$name" pg_isready -U postgres >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]]
docker exec "$name" createdb -U postgres bracc_u05_journal
for file in "$root"/infra/postgres/migrations/*.sql; do docker exec -i "$name" psql -X -q -U postgres -d bracc_u05_journal -v ON_ERROR_STOP=1 < "$file"; done
docker exec -i "$name" psql -X -q -U postgres -d bracc_u05_journal -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE bracc_u05_runtime LOGIN PASSWORD 'synthetic-only';
GRANT bracc_evidence_journal TO bracc_u05_runtime;
INSERT INTO control.source VALUES ('journal-source','public'),('journal-source-alt','public');
INSERT INTO control.dataset VALUES ('journal-dataset','journal-source','v1','public'),('journal-dataset-alt','journal-source-alt','v1','public'),('journal-dataset-v2','journal-source','v2','public'),('journal-dataset-other','journal-source','v1','public');
INSERT INTO control.partition VALUES ('journal-partition','journal-dataset'),('journal-partition-alt-source','journal-dataset-alt'),('journal-partition-alt-contract','journal-dataset-v2'),('journal-partition-alt-dataset','journal-dataset-other');
INSERT INTO control.artifact VALUES
 ('48f61f34-b677-4fbd-a24d-c3acb928c119','journal-partition-alt-contract','raw/journal-source/journal-dataset-v2/journal-partition-alt-contract/e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','public'),
 ('48f61f34-b677-4fbd-a24d-c3acb928c117','journal-partition','raw/journal-source/journal-dataset/journal-partition/e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','public'),

 ('48f61f34-b677-4fbd-a24d-c3acb928c118','journal-partition-alt-source','raw/journal-source-alt/journal-dataset-alt/journal-partition-alt-source/e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','public'),
 ('48f61f34-b677-4fbd-a24d-c3acb928c11a','journal-partition-alt-dataset','raw/journal-source/journal-dataset-other/journal-partition-alt-dataset/e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e','public');
SQL
PYTHONPATH="$root/etl/src" "$python_bin" "$root/etl/tests/control/evidence_journal_postgres.py" "127.0.0.1" "$port" bracc_u05_journal
