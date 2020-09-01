#!/bin/sh
set -e

APP_TO_RUN="$1"

if [ "x$APP_TO_RUN" = 'xcron' ]; then

    exec  su celery -c  "/venv/bin/celery -A cimplewave beat -l info -s /runtime/celerybeat-schedule --pidfile=/runtime/celerybeat.pid"

fi

if [ "x$APP_TO_RUN" = 'xworker' ]; then

    exec su celery -c  "/venv/bin/celery -A cimplewave worker -P gevent --concurrency=10 -l info -Ofair"

fi

