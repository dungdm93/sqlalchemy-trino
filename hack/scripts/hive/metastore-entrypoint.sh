#!/bin/bash
set -euo pipefail
HERE=$(dirname "${BASH_SOURCE[0]}")

DB_TYPE="postgres"
DB_HOST="postgres"
DB_PORT="5432"

if [ -n "$DB_HOST" ]; then
    "${HERE}/wait-for" "$DB_HOST" "$DB_PORT"
fi

schematool -dbType "${DB_TYPE}" -upgradeSchema -verbose ||
schematool -dbType "${DB_TYPE}" -initSchema -verbose

# exec "$@"
exec hive --service metastore --verbose --hiveconf hive.root.logger=INFO,console
