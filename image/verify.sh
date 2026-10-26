#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: image/verify.sh IMAGE ARCHITECTURE}
architecture=${2:?expected architecture}
evidence=${RUNTIME_EVIDENCE_DIR:-image-evidence}
mkdir -p "$evidence"
docker image inspect "$image" > "$evidence/image-inspect.json"
test "$(docker image inspect "$image" --format '{{.Config.User}}')" = 65532:65532
test "$(docker image inspect "$image" --format '{{.Architecture}}')" = "$architecture"
test "$(docker image inspect "$image" --format '{{json .Config.Entrypoint}}')" = '["/main"]'
test "$(docker image inspect "$image" --format '{{.Config.WorkingDir}}')" = /
container=$(docker create "$image")
trap 'docker rm "$container" >/dev/null' EXIT
docker export "$container" -o "$evidence/rootfs.tar"
tar -tf "$evidence/rootfs.tar" > "$evidence/rootfs-files.txt"
tar --numeric-owner -tvf "$evidence/rootfs.tar" > "$evidence/rootfs-permissions.txt"
if grep -E '(^|/)(s?bin)/(sh|bash|busybox|curl|wget|apt|apt-get|apk|dpkg|gcc|go)$' "$evidence/rootfs-files.txt"; then
  echo "Prohibited runtime executable" >&2
  exit 1
fi
if grep -E '^[-dlcbp][rwxStTs-]{2}[sS]|^[-dlcbp][rwxStTs-]{5}[sS]' "$evidence/rootfs-permissions.txt"; then
  echo "Unexpected setuid/setgid file" >&2
  exit 1
fi
grep -Eq '^-r-xr-xr-x 0/0 .* main$' "$evidence/rootfs-permissions.txt"
while IFS= read -r fixture; do
  tar -xOf "$evidence/rootfs.tar" "indexes/$(basename "$fixture")" | cmp "$fixture" -
  grep -Eq "^-r--r--r-- 0/0 .* indexes/$(basename "$fixture")$" "$evidence/rootfs-permissions.txt"
done < <(find indexes -type f -name '*.txt' | sort)

# --help exits in flag parsing, before dispatching any migration or queue action.
timeout 30s docker run --rm --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 64 --memory 256m --cpus 1 \
  "$image" --help > "$evidence/smoke.log" 2>&1
grep -q 'Action to take' "$evidence/smoke.log"
