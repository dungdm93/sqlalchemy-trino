#!/bin/bash
set -euo pipefail
HERE=$(dirname "${BASH_SOURCE[0]}")

METASTORE_HOST="hive-metastore"
METASTORE_PORT="9083"

"${HERE}/wait-for" "$METASTORE_HOST" "$METASTORE_PORT"

# exec "$@"
exec hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console
