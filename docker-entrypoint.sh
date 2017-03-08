#!/bin/bash
set -e

if [ "$1" == "errybody" ]; then
	shift 1
	exec node /usr/src/app/index.js "$@"
fi

exec "$@"
