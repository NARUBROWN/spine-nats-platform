"""실제 NATS 및 TLS/인증과 애플리케이션 파이프라인의 누락과 skip을 거부합니다."""

import json
from pathlib import Path
import sys


def check(path, required):
    events = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    passed = {event.get("Test") for event in events if event.get("Action") == "pass"}
    if not required <= passed or any(event.get("Action") == "fail" for event in events):
        raise SystemExit("필수 NATS 실제 실행 검사가 실패하거나 누락됐습니다")
    if any(event.get("Action") == "skip" and event.get("Test") for event in events):
        raise SystemExit("NATS 실제 실행 검사를 건너뛸 수 없습니다")


if __name__ == "__main__":
    check(sys.argv[1], {
        "TestLiveRoundTripAndReadiness", "TestLiveFailurePolicies",
        "TestLiveDurableRestartAndConfigurationConflict", "TestLiveSharedConsumerAndIndependentGroup",
        "TestLiveUnsettledRedeliveryAndMaxDeliver", "TestLiveForeignHeadersCannotChangeSubscription",
        "TestLiveReadCancellationAndClose", "TestLiveConcurrentPublishAndClose",
        "TestLiveMissingStreamAndWrongPublicationTarget", "TestLiveTLSAndAuthentication",
    })
    check(sys.argv[2], {"TestHTTPPublishWorkerRedeliveryAndResult"})
