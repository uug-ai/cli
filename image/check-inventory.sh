#!/usr/bin/env bash
set -euo pipefail

evidence=${1:?usage: image/check-inventory.sh EVIDENCE_DIRECTORY}
native="$evidence"
if [ -d "$evidence/runtime-evidence" ]; then
  native="$evidence/runtime-evidence"
fi

awk '
  BEGIN { RS = ""; FS = "\n"; OFS = "\t" }
  {
    package = ""; version = ""
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^P:/) package = substr($i, 3)
      if ($i ~ /^V:/) version = substr($i, 3)
    }
    if (package != "" && version != "") print package, version
  }
' "$native/apk-installed" | sort -u > "$evidence/native-packages.tsv"
test -s "$evidence/native-packages.tsv"
grep -q $'^librdkafka\t2.16.0-r0$' "$evidence/native-packages.tsv"

jq -er '.Metadata.OS.Family | select(. == "wolfi")' "$evidence/image-security.json"
jq -r '.Results[]? | select(.Class == "os-pkgs") | .Packages[]? |
  [.Name, .Version] | @tsv' "$evidence/image-security.json" \
  | sort -u > "$evidence/scanned-native-packages.tsv"
jq -r '.packages[] | select(any(.externalRefs[]?;
  (.referenceLocator // "") | startswith("pkg:apk/"))) |
  [.name, .versionInfo] | @tsv' "$evidence/image.spdx.json" \
  | sort -u > "$evidence/spdx-native-packages.tsv"
cmp "$evidence/native-packages.tsv" "$evidence/scanned-native-packages.tsv"
cmp "$evidence/native-packages.tsv" "$evidence/spdx-native-packages.tsv"
test -s "$native/native-dependency-owners.tsv"
test "$(wc -l < "$native/native-needed.txt")" -eq "$(wc -l < "$native/native-dependency-owners.tsv")"
jq -e '([.Results[]? | ((.Vulnerabilities[]? |
  select(.Severity == "MEDIUM" or .Severity == "HIGH" or .Severity == "CRITICAL")), .Secrets[]?)] +
  [.Results[]? | .Misconfigurations[]? |
  select(.Status == "FAIL" and (.Severity == "HIGH" or .Severity == "CRITICAL"))]) |
  length == 0' "$evidence/image-security.json"
