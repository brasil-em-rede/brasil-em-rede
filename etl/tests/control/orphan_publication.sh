#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
python_bin=${BRACC_TEST_PYTHON:-/usr/bin/python3.12}
"$python_bin" -c 'import psycopg' >/dev/null
name="bracc-m0-20261119-u03-orphan-${$}-${RANDOM}"
if docker container inspect "$name" >/dev/null 2>&1; then
  printf 'Refusing pre-existing container %s\n' "$name" >&2
  exit 1
fi
scratch=${TMPDIR:?Set TMPDIR to a writable scratch directory}
mkdir -p "$scratch"
tmp=$(mktemp -d "$scratch/u03-orphan.XXXXXX")
chmod 755 "$tmp"
mkdir "$tmp/socket"
chmod 777 "$tmp/socket"
container_id=
cleanup() {
  status=$?
  trap - EXIT
  cleanup_status=0
  if [[ -n "$container_id" ]]; then
    docker stop -t 5 "$container_id" >/dev/null || cleanup_status=$?
    docker rm -v "$container_id" >/dev/null || cleanup_status=$?
    if docker container inspect "$container_id" >/dev/null 2>&1; then
      printf 'Created container remains after cleanup: %s\n' "$container_id" >&2
      cleanup_status=1
    fi
  fi
  rm -rf -- "$tmp" || cleanup_status=$?
  [[ ! -e "$tmp" ]] || cleanup_status=1
  if (( status == 0 && cleanup_status != 0 )); then status=$cleanup_status; fi
  return "$status"
}
trap cleanup EXIT
container_id=$(docker run -d --name "$name" --network none \
  -v "$tmp/socket:/var/run/postgresql" -e POSTGRES_PASSWORD=synthetic-only postgres:16-alpine)
ready=false
for _ in {1..60}; do
  if docker logs "$container_id" 2>&1 | grep -Fq \
      'PostgreSQL init process complete; ready for start up.' \
      && docker exec "$container_id" pg_isready -U postgres >/dev/null 2>&1; then
    sleep 1
    if docker exec "$container_id" pg_isready -U postgres >/dev/null 2>&1; then
      ready=true
      break
    fi
  fi
  sleep 1
done
[[ "$ready" == true ]] || { echo "PostgreSQL readiness timeout" >&2; exit 1; }
docker exec "$container_id" createdb -U postgres bracc_m0_u03_orphans
for file in "$root"/infra/postgres/migrations/*.sql; do
  docker exec -i "$container_id" psql -X -q -U postgres -d bracc_m0_u03_orphans \
    -v ON_ERROR_STOP=1 < "$file"
done
docker exec -i "$container_id" psql -X -q -U postgres -d bracc_m0_u03_orphans \
  -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO control.source VALUES ('cnpj','public');
INSERT INTO control.dataset VALUES ('cnpj','cnpj','v1','public');
INSERT INTO control.partition VALUES ('2026-01','cnpj'),('2026-02','cnpj'),('2026-03','cnpj');
INSERT INTO control.worker_identity VALUES ('postgres','00000000-0000-0000-0000-000000000003');
SQL
PYTHONPATH="$root/etl/src" "$python_bin" \
  "$root/etl/tests/control/orphan_publication_postgres.py" "$tmp/socket"
