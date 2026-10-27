#!/usr/bin/env bash
set -euo pipefail

owners=${1:?APK file-owner catalog required}
needed=${2:?native dependency names required}
if [ ! -s "$owners" ]; then
  echo "APK file-owner catalog is empty: $owners" >&2
  exit 1
fi
if [ ! -s "$needed" ]; then
  echo "Native dependency catalog is empty: $needed" >&2
  exit 1
fi
awk '
  FNR == NR {
    count = split($1, path, "/")
    owner[path[count]] = $0
    next
  }
  !($0 in owner) {
    print "No installed APK owns required library " $0 > "/dev/stderr"
    missing = 1
    next
  }
  { print owner[$0] }
  END { exit missing }
' "$owners" "$needed"
