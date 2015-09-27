#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail
shopt -s nullglob

java -jar $AVSC_JAVA_SCRIPTS encode $1 $2
