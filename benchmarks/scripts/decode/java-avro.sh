#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail
shopt -s nullglob

java -jar $AVSC_JAVA_SCRIPTS decode $1 $2
