#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: image/verify.sh IMAGE ARCHITECTURE}
architecture=${2:?expected architecture}
mkdir -p image-evidence
docker image inspect "$image" > image-evidence/image-inspect.json
test "$(docker image inspect "$image" --format '{{.Config.User}}')" = 65532:65532
test "$(docker image inspect "$image" --format '{{.Architecture}}')" = "$architecture"
test "$(docker image inspect "$image" --format '{{json .Config.Entrypoint}}')" = '["/main"]'
test "$(docker image inspect "$image" --format '{{.Config.WorkingDir}}')" = /
container=$(docker create "$image")
trap 'docker rm "$container" >/dev/null' EXIT
docker export "$container" -o image-evidence/rootfs.tar
tar -tf image-evidence/rootfs.tar > image-evidence/rootfs-files.txt
tar --numeric-owner -tvf image-evidence/rootfs.tar > image-evidence/rootfs-permissions.txt
if grep -E '(^|/)(s?bin)/(sh|bash|busybox|curl|wget|apt|apt-get|apk|dpkg|gcc|go)$' image-evidence/rootfs-files.txt; then
  echo "Prohibited runtime executable" >&2
  exit 1
fi
if grep -E '^[-dlcbp][rwxStTs-]{2}[sS]|^[-dlcbp][rwxStTs-]{5}[sS]' image-evidence/rootfs-permissions.txt; then
  echo "Unexpected setuid/setgid file" >&2
  exit 1
fi
grep -Eq '^-r-xr-xr-x 0/0 .* main$' image-evidence/rootfs-permissions.txt
while IFS= read -r fixture; do
  tar -xOf image-evidence/rootfs.tar "indexes/$(basename "$fixture")" | cmp "$fixture" -
  grep -Eq "^-r--r--r-- 0/0 .* indexes/$(basename "$fixture")$" image-evidence/rootfs-permissions.txt
done < <(find indexes -type f -name '*.txt' | sort)

# --help exits in flag parsing, before dispatching any migration or queue action.
timeout 30s docker run --rm --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 64 --memory 256m --cpus 1 \
  "$image" --help > image-evidence/smoke.log 2>&1
grep -q 'Action to take' image-evidence/smoke.log
