#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
if [[ -n "${BRACC_TEST_PYTHON:-}" ]]; then python_bin=$BRACC_TEST_PYTHON; else python_bin="$root/etl/.venv/bin/python"; fi
"$python_bin" -c 'import psycopg' >/dev/null
name="bracc-f05-lifecycle-${$}-${RANDOM}"
printf 'F05 isolated container=%s database=bracc_f05_lifecycle owner=00000000-0000-0000-0000-000000000003\n' "$name"
image="${name}:test"
# Refuse pre-existing names/tags; never infer ownership from our chosen names.
if docker container inspect "$name" >/dev/null 2>&1 || docker image inspect "$image" >/dev/null 2>&1; then
  printf 'Refusing pre-existing Docker resource name/tag\n' >&2
  exit 1
fi
tmp=$(mktemp -d)
chmod 755 "$tmp"
mkdir "$tmp/socket"
chmod 777 "$tmp/socket"
container_id=
cleanup() {
  status=$?
  trap - EXIT
  if [[ -n "$container_id" ]]; then docker stop -t 5 "$container_id" >/dev/null 2>&1 || true; docker rm -v "$container_id" >/dev/null 2>&1 || true; fi
  docker image rm "$image" >/dev/null 2>&1 || true
  rm -rf "$tmp"
  return "$status"
}
trap cleanup EXIT
docker build -t "$image" "$root/infra/postgres" >/dev/null
container_id=$(docker run -d --name "$name" --network none -v "$tmp/socket:/var/run/postgresql" -e POSTGRES_PASSWORD=synthetic-only "$image")
ready=false
for _ in {1..60}; do
  if docker logs "$name" 2>&1 | grep -Fq 'PostgreSQL init process complete; ready for start up.' && docker exec "$name" pg_isready -U postgres >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]]
docker exec "$name" createdb -U postgres bracc_f05_lifecycle
for file in "$root"/infra/postgres/migrations/*.sql; do docker exec -i "$name" psql -X -q -U postgres -d bracc_f05_lifecycle -v ON_ERROR_STOP=1 < "$file"; done
docker exec -i "$name" psql -X -q -U postgres -d bracc_f05_lifecycle -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO control.source VALUES ('cnpj','public');
INSERT INTO control.dataset VALUES ('cnpj','cnpj','v1','public');
INSERT INTO control.partition VALUES ('2026-01','cnpj'),('2026-02','cnpj'),('2026-03','cnpj'),('2026-04','cnpj');
INSERT INTO control.worker_identity VALUES ('postgres','00000000-0000-0000-0000-000000000003');
SQL
PYTHONPATH="$root/etl/src" "$python_bin" "$root/etl/tests/control/acquisition_lifecycle_postgres.py" "$tmp/socket"
