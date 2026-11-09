# spine-nats-platform

Spine의 `spine.event-adapter/v1` 계약으로 NATS JetStream 이벤트를 발행하고 소비합니다. Kafka, RabbitMQ와 같은 `spine.WithEventAdapter`로 연결하며 코어에 NATS 의존성을 추가하지 않습니다.

## Preview 설치

개발 중인 기능을 시험하고 호환성 문제를 확인하는 채널입니다. 공개 태그는 [`v0.1.0-preview.2`](https://github.com/NARUBROWN/spine-nats-platform/tree/v0.1.0-preview.2)입니다. 이 태그의 `go.mod`는 Spine `v0.7.0-preview.2`와 Go 1.26.0 이상을 지정합니다.

```sh
go get github.com/NARUBROWN/spine-nats-platform@v0.1.0-preview.2-preview.2
```

다른 채널은 [RC](https://github.com/NARUBROWN/spine-nats-platform/tree/rc) | [Stable](https://github.com/NARUBROWN/spine-nats-platform/tree/main)에서 확인하세요.


Go 1.26.0 이상, Spine `v0.7.0-preview.2`, `nats.go v1.55.0`을 사용합니다. 실제 서버 검증 버전은 NATS `2.12.1`입니다.

[전환 안내](docs/migration/v0.1.0.md)에서 초기 구성을, [릴리스 운영](docs/releasing.md)에서 발행 절차를 확인하세요.

CLI에서 NATS를 선택하려면 설치한 버전의 `spine new --help`와 [플랫폼 안내](https://github.com/NARUBROWN/spine-cli/blob/preview/PLATFORMS.md)를 확인하세요. 아래 예제는 앱에 factory를 직접 등록합니다.

## 앱에 연결하기

JetStream을 활성화하고 `order.created`를 포함하는 `EVENTS` stream을 먼저 생성하세요. stream의 보존 기간, 저장 방식, 복제 수와 용량은 운영자가 설정합니다. 플랫폼은 stream을 생성하거나 기존 consumer 설정을 덮어쓰지 않습니다.

```go
import (
    "github.com/NARUBROWN/spine"
    natsplatform "github.com/NARUBROWN/spine-nats-platform"
)

factory, err := natsplatform.NewFactory(natsplatform.Config{
    URLs:           []string{"tls://nats.example.com:4222"},
    Stream:         "EVENTS",
    ConsumerPrefix: "orders",
    Consume:        true,
    Publish:        true,
    FailurePolicy:  natsplatform.FailureRequeue,
})
if err != nil {
    return err
}
app := spine.New(spine.WithEventAdapter("nats", factory))
```

HTTP 앱은 `spine.New(echoplatform.Option(), spine.WithEventAdapter("nats", factory))`로 구성합니다. HTTP가 없는 worker는 HTTP 플랫폼을 선택하지 않습니다. 도메인 이벤트는 기존 `publish.Event(ctx, value)`로 발행하고, 소비자는 `app.Consumers().Register("order.created", (*OrderConsumer).Handle)`로 등록합니다.

## 설정

| 필드 | 기본값 | 동작과 제약 |
| --- | --- | --- |
| `URLs` | 필수 | `nats://` 또는 `tls://` 주소 목록. URL의 자격 증명, 경로, 쿼리는 허용하지 않음 |
| `Stream` | 필수 | 미리 생성한 stream 이름. 점, 공백, 제어 문자, wildcard와 경로 구분자는 허용하지 않음 |
| `Consume`, `Publish` | `false` | 하나 이상의 역할 필요 |
| `ConsumerPrefix` | 소비 시 필수 | consumer의 서비스 또는 그룹 이름. 이름 제약은 `Stream`과 같음 |
| `TopicPrefix` | 없음 | 예: `prod.`. 비어 있지 않으면 점으로 끝나는 literal subject 접두사 |
| `FailurePolicy` | `FailureReject` | `FailureReject`는 TERM으로 재전달 중단, `FailureRequeue`는 NAK로 재전달 요청 |
| `RequeueDelay` | `0` | requeue 대기 시간. 0이면 즉시 요청 |
| `AckWait` | `30s` | ACK가 없을 때 서버가 재전달할 때까지 기다리는 시간 |
| `MaxDeliver` | `-1` | 0도 무제한으로 해석. 양수는 최초 전달을 포함한 서버의 최대 전달 횟수 |
| `User`, `Password` | 없음 | 사용자 인증. 비밀번호만 설정할 수 없음 |
| `Token` | 없음 | 토큰 인증 |
| `CredentialsFile` | 없음 | NATS credentials 파일. 인증 방식은 하나만 선택 |
| `TLS` | TLS 1.2 이상 | `tls.Config` 복사본. 기본 인증서 검증 유지 |
| `AllowInsecureTransport` | `false` | 로컬 평문 연결에만 `true` 사용. TLS와 함께 사용할 수 없음 |
| `StartupTimeout` | `10s` | 연결과 readiness 제한 시간 |
| `OperationTimeout` | `5s` | 발행, ACK와 NACK 제한 시간 |

음수 시간은 거부합니다. 구독 topic과 이벤트 이름은 비어 있지 않은 literal NATS subject여야 하며 `*`, `>`, 빈 토큰과 공백을 허용하지 않습니다. `TopicPrefix: "prod."`이면 논리 이름 `order.created`를 `prod.order.created`로 매핑합니다. 소비자 등록에서 접두사를 반복하지 마세요.

## 전달과 종료

- 발행은 지정한 stream의 저장 확인을 기다립니다. 성공 ACK는 `DoubleAck`로 서버 확인을 기다립니다. NACK는 TERM 또는 NAK를 보낸 뒤 연결의 flush를 기다립니다.
- consumer 이름은 `ConsumerPrefix`와 매핑한 subject의 SHA-256 앞 128비트로 정합니다. 같은 stream, prefix와 topic을 쓰는 reader는 durable consumer를 공유합니다. 다른 prefix는 독립적인 소비 위치를 가집니다.
- `NewReader`는 consumer를 준비하되 메시지를 가져오지 않습니다. 첫 `Read`에서 pull을 시작하며 한 번에 한 메시지를 요청합니다. `MaxAckPending`은 consumer 전체에서 1입니다. handler와 후처리 뒤 ACK 또는 NACK를 한 번 호출하세요.
- `Close`는 새 작업을 막고 진행 중인 작업을 기다립니다. reader 종료는 대기 중인 `Read`를 해제합니다. 종료 대기 context가 만료돼도 정리는 계속되며 다음 `Close`에서 다시 기다릴 수 있습니다. durable consumer와 stream은 삭제하지 않습니다.
- native 자동 재연결은 끕니다. 연결 실패 시 Spine의 공통 재생성 정책을 사용합니다. 실패한 발행 메시지를 자동으로 다시 보내지 않습니다.
- `Spine-Occurred-At`과 JSON 형식의 `Spine-Metadata` 헤더에 시간과 메타데이터를 넣으며 payload를 재직렬화하지 않습니다. 외부 생산자가 잘못된 헤더를 보내면 해당 헤더를 무시합니다. 시간 헤더가 없거나 잘못되면 서버 저장 시간을 사용합니다. 수신 이벤트 이름은 등록된 논리 topic으로 고정합니다.

이 구현은 **JetStream 전용**입니다. Core NATS pub/sub, request/reply, wildcard 소비와 자동 DLQ 구성은 포함하지 않습니다. 메시지 순서는 병렬 reader와 재전달로 달라질 수 있습니다. ACK가 유실되거나 처리가 `AckWait`을 넘으면 중복 실행될 수 있으므로 소비자를 멱등하게 작성하세요. 자동 `InProgress` heartbeat는 보내지 않습니다. `MaxDeliver`에 도달하거나 TERM을 보내도 자동으로 DLQ에 이동하지 않으며, 메시지 보존은 stream 정책을 따릅니다. DB commit과 브로커 발행의 원자성, exactly-once 업무 처리도 보장하지 않습니다.

## 로컬 예제와 검증

저장소 루트에서 실행합니다. `nats` CLI는 [공식 설치 안내](https://github.com/nats-io/natscli#installation)를 따르세요.

```sh
docker run --rm --name spine-nats -p 127.0.0.1:4222:4222 nats:2.12.1-alpine -js
```

다른 터미널에서 stream을 만들고 worker를 시작합니다.

```sh
nats --server nats://127.0.0.1:4222 stream add EVENTS \
  --subjects 'order.>' --storage file --retention limits --defaults
NATS_URL=nats://127.0.0.1:4222 NATS_STREAM=EVENTS NATS_ALLOW_INSECURE=true \
  go -C examples/worker run .
```

`nats --server nats://127.0.0.1:4222 pub order.created '{"id":42}'`를 보내면 worker가 `주문 수신: id=42`를 출력합니다. 종료 시 `Ctrl+C`를 사용합니다.

```sh
go test -race ./... -count=1
bash scripts/check-module-boundary.sh
SPINE_TEST_NATS_URL=nats://127.0.0.1:4222 go test -race ./... -count=1
SPINE_TEST_NATS_URL=nats://127.0.0.1:4222 go -C e2e test -race ./... -count=1
```

환경 변수가 없으면 실제 서버 검사는 건너뜁니다. TLS 검사는 `SPINE_TEST_NATS_TLS_URL`, `SPINE_TEST_NATS_TLS_CA`, `SPINE_TEST_NATS_USER`, `SPINE_TEST_NATS_PASSWORD`로 별도의 테스트 서버를 지정합니다. CI 구성에는 로컬 CA와 사용자 인증 서버를 포함했으며 인증 실패와 신뢰하지 않는 인증서도 검사합니다.

E2E는 HTTP 요청, 도메인 이벤트 발행, worker 실패, 재전달, 결과 이벤트와 정상 종료를 검사합니다. 단일 로컬 서버 검증이며 클러스터 장애 복구, 실제 운영 인증서와 계정, 처리량과 대용량 성능 검증은 포함하지 않습니다.

CI의 `generated-project` 작업은 고정한 CLI 커밋으로 `spine new --event-platform events:nats`를 실행합니다. 이 저장소의 소스를 별도 모듈 프록시와 캐시에 넣으며, 생성한 `go.mod`에 `replace`를 추가하지 않습니다. 두 레이아웃에서 HTTP 앱과 worker 네 조합의 정적 계약, 부분 runtime probe, 실제 NATS 전달, ACK와 잘못된 DTO의 reject, 정상 종료 및 소스를 보존하는 update를 검사합니다.

CLI에 등록하지 않은 다른 이벤트 이름과 설정형 validator를 메타데이터로 연결한 네 조합도 검사합니다. 다른 생성자와 설정 구조체, 간접 validator 의존성까지 포함하며 총 여덟 조합의 누락이나 skip을 실패로 처리합니다. 이 경로는 현재 checkout 검증이며 공개 모듈 설치 증거는 아닙니다.

전달 보장과 pull consumer 배경은 [NATS JetStream 문서](https://docs.nats.io/learn/jetstream/)와 [공식 Go client](https://github.com/nats-io/nats.go/tree/v1.55.0/jetstream)를 참고하세요.

## 릴리스와 공개 계약

이 플랫폼은 자체 버전과 채널로 발행합니다. `go.mod`의 Spine 버전은 사용하는 공개 계약의 최소 의존성이며 코어 릴리스 채널과 별도로 관리합니다. CI와 공개 설치 검증은 이 모듈의 의존성으로 플랫폼 자체를 검사합니다. 새로운 코어와 기존 플랫폼 태그의 조합 검증은 코어 릴리스에서 수행합니다.

CLI 생성 앱 조합 검사는 선택형으로 실행하며 플랫폼 자체 발행 성공을 결정하지 않습니다. JetStream을 사용하는 플랫폼 자체 검사는 필수로 유지합니다.
