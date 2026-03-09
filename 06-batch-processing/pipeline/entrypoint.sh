#!/usr/bin/env bash
set -euo pipefail

py4j_jars=("$SPARK_HOME"/python/lib/py4j*.zip)
if [ ${#py4j_jars[@]} -eq 0 ]; then
	echo "No se encontró py4j*.zip en $SPARK_HOME/python/lib" >&2
	exit 1
fi

export PYTHONPATH="$SPARK_HOME/python:${py4j_jars[0]}${PYTHONPATH:+:$PYTHONPATH}"

exec "$@"