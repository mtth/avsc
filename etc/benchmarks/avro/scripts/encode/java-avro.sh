#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail
shopt -s nullglob

java -jar $AVSC_JAR encode $1 10
