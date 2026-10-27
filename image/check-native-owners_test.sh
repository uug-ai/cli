#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
fixtures=.hardening-work/native-owner-tests
mkdir -p "$fixtures"
trap 'rm -rf -- "$fixtures"' EXIT
printf '/usr/lib/libfixture.so\tfixture-package\t1.0\n' > "$fixtures/owners"
printf 'libfixture.so\n' > "$fixtures/needed"
bash image/check-native-owners.sh "$fixtures/owners" "$fixtures/needed" > "$fixtures/result"
cmp "$fixtures/owners" "$fixtures/result"

: > "$fixtures/empty"
if bash image/check-native-owners.sh "$fixtures/empty" "$fixtures/needed" > "$fixtures/result" 2> "$fixtures/error"; then
  echo "Empty ownership catalog incorrectly passed" >&2
  exit 1
fi
grep -q 'APK file-owner catalog is empty' "$fixtures/error"

printf 'libmissing.so\n' >> "$fixtures/needed"
if bash image/check-native-owners.sh "$fixtures/owners" "$fixtures/needed" > "$fixtures/result" 2> "$fixtures/error"; then
  echo "Missing native owner incorrectly passed" >&2
  exit 1
fi
grep -q 'No installed APK owns required library libmissing.so' "$fixtures/error"
echo "Native ownership positive, empty-catalog and missing-owner checks passed"
