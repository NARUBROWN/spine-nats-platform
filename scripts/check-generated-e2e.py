"""CLI가 만든 NATS 앱 네 조합의 실제 실행을 확인합니다."""

import json
from pathlib import Path
import sys

events = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines() if line.strip()]
names = ("TestGeneratedNATSAssemblyE2E", "TestGeneratedDynamicPlatformE2E")
required = set(names) | {f"{name}/{layout}/{transport}" for name in names for layout in ("layered-v1", "legacy") for transport in ("echo", "none")}
passed = {event.get("Test") for event in events if event.get("Action") == "pass"}
if any(event.get("Action") == "fail" for event in events) or not required <= passed:
    raise SystemExit("필수 CLI NATS 생성 앱 검사가 실패하거나 누락됐습니다")
if any(event.get("Action") == "skip" and any(event.get("Test", "").startswith(name) for name in names) for event in events):
    raise SystemExit("CLI NATS 생성 앱 검사를 건너뛸 수 없습니다")
