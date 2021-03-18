#!/bin/bash
set -eo pipefail

case "$1" in
    webserver)
        superset-tools wait database
        superset db upgrade
        # https://docs.gunicorn.org/en/stable/settings.html
        if [ -n "$GUNICORN_CONFIG_PATH" ]; then
            GUNICORN_CMD_ARGS="--config=$GUNICORN_CONFIG_PATH ${GUNICORN_CMD_ARGS}"
        fi
        exec gunicorn --bind=0.0.0.0:8088 \
            ${GUNICORN_CMD_ARGS} \
            "superset.app:create_app()"
        ;;
    worker|beat|flower)
        superset-tools wait broker
        # https://superset.apache.org/docs/installation/async-queries-celery
        # https://docs.celeryproject.org/en/stable/userguide/configuration.html
        # https://docs.celeryproject.org/en/stable/reference/cli.html#celery-worker
        if [ -n "$CELERY_CONFIG_PATH" ]; then
            CELERY_CMD_ARGS="--config=$CELERY_CONFIG_PATH ${CELERY_CMD_ARGS}"
        fi
        exec celery "$1" ${CELERY_CMD_ARGS} \
            "--app=superset.tasks.celery_app:app"
        ;;
    version)
        superset version
        ;;
    *)
        exec "$@"
        ;;
esac
