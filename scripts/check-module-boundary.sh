#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# 플랫폼은 공개 이벤트 계약만 가져오며 커널 구현과 다른 플랫폼에 의존하지 않습니다.
imports=$(go list -deps -f '{{.ImportPath}}' .)
unexpected=$(printf '%s\n' "$imports" | awk '
  /^github.com\/NARUBROWN\// &&
  $0 != "github.com/NARUBROWN/spine/contract/event" &&
  $0 != "github.com/NARUBROWN/spine-nats-platform" { print }
')
if [ -n "$unexpected" ]; then
  printf 'Unexpected framework dependency:\n%s\n' "$unexpected" >&2
  exit 1
fi
