#!/usr/bin/env bash
set -euo pipefail

image=${1:?image reference required}
evidence=${RUNTIME_EVIDENCE_DIR:-runtime-evidence}
mkdir -p "$evidence"
container=$(docker create "$image")
trap 'docker rm "$container" > /dev/null' EXIT

docker cp "$container:/lib/apk/db/installed" "$evidence/apk-installed"
docker cp "$container:/usr/lib" "$evidence/native-libraries"
docker cp "$container:/main" "$evidence/main"

readelf --dynamic "$evidence/main" > "$evidence/native-elf.txt"
readelf --program-headers "$evidence/main" > "$evidence/program-headers.txt"
grep -q 'Shared library: \[librdkafka.so.1\]' "$evidence/native-elf.txt"
: > "$evidence/native-non-elf.txt"
while IFS= read -r library; do
  magic=$(od -An -tx1 -N4 "$library" | tr -d '[:space:]')
  if [ "$magic" = 7f454c46 ]; then
    readelf --dynamic "$library" >> "$evidence/native-elf.txt"
  else
    sha256sum "$library" >> "$evidence/native-non-elf.txt"
  fi
done < <(find "$evidence/native-libraries" -type f -name '*.so*' | sort)

awk '
  /^P:/ { package = substr($0, 3) }
  /^V:/ { version = substr($0, 3) }
  /^F:/ { directory = substr($0, 3) }
  /^R:/ { print "/" directory "/" substr($0, 3) "\t" package "\t" version }
' "$evidence/apk-installed" > "$evidence/apk-file-owners.tsv"

awk '/NEEDED/ {
  sub(/^.*\[/, "")
  sub(/\].*$/, "")
  print
}' "$evidence/native-elf.txt" | sort -u > "$evidence/native-needed.txt"
awk '/Requesting program interpreter:/ {
  sub(/^.*: /, "")
  sub(/\].*$/, "")
  sub(/^.*\//, "")
  print
}' "$evidence/program-headers.txt" >> "$evidence/native-needed.txt"
sort -u -o "$evidence/native-needed.txt" "$evidence/native-needed.txt"

bash image/check-native-owners.sh "$evidence/apk-file-owners.tsv" "$evidence/native-needed.txt" \
  | tee "$evidence/native-dependency-owners.tsv"
