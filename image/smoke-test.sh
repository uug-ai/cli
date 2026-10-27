#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: image/smoke-test.sh IMAGE}
case "$(uname -m)" in
  x86_64) architecture=amd64 ;;
  aarch64|arm64) architecture=arm64 ;;
  *) echo "Unsupported native CLI architecture" >&2; exit 1 ;;
esac
cd "$(dirname "$0")/.."
export RUNTIME_EVIDENCE_DIR=${RUNTIME_EVIDENCE_DIR:-runtime-evidence}
bash image/verify.sh "$image" "$architecture"
bash image/native-inventory.sh "$image"
