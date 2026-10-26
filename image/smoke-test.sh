#!/usr/bin/env bash
set -euo pipefail

image=${1:?usage: image/smoke-test.sh IMAGE}
case "$(uname -m)" in
  x86_64) architecture=amd64 ;;
  aarch64|arm64) architecture=arm64 ;;
  *) echo "Unsupported native CLI architecture" >&2; exit 1 ;;
esac
cd "$(dirname "$0")/.."
exec bash image/verify.sh "$image" "$architecture"
