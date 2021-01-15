#/bin/bash
HERE="$(dirname "${BASH_SOURCE[0]}")"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

createdb   --username=${POSTGRES_USER} --no-password    dvdrental
pg_restore --username=${POSTGRES_USER} --no-password -d dvdrental $HERE/dvdrental.tar
