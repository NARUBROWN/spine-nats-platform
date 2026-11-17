English | [Korean](README.ko.md)

# spine-nats-platform

Publish and consume NATS JetStream events through Spine's `spine.event-adapter/v1` contract. Connect with `spine.WithEventAdapter`, as with Kafka and RabbitMQ, without adding NATS dependencies to the core.

## Preview installation

Use this channel to try features under development and check compatibility. The published tag is [`v0.1.0-preview.2`](https://github.com/NARUBROWN/spine-nats-platform/tree/v0.1.0-preview.2). Its `go.mod` specifies Spine `v0.7.0-preview.2` and Go 1.26.0 or later. The release policy on this branch targets `v0.1.0`.

```sh
go get github.com/NARUBROWN/spine-nats-platform@v0.1.0-preview.2
```

Other channels: [RC](https://github.com/NARUBROWN/spine-nats-platform/tree/rc) | [Stable](https://github.com/NARUBROWN/spine-nats-platform/tree/main).

Uses Go 1.26.0 or later, Spine `v0.7.0-preview.2`, and `nats.go v1.55.0`. Real server tests use NATS `2.12.1`.

See the [migration guide](docs/migration/v0.1.0.md) for initial setup and [release operations](docs/releasing.md) for publication procedures.

To select NATS in the CLI, check your installed version's `spine new --help` and the [platform guide](https://github.com/NARUBROWN/spine-cli/blob/preview/PLATFORMS.md). The example below registers the factory directly.

## Application setup

Enable JetStream and create an `EVENTS` stream containing `order.created` first. Operators configure retention, storage, replication, and capacity. The platform neither creates streams nor overwrites existing consumer configuration.

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
    PublishDestinations: destinations.Catalog{},
    FailurePolicy:  natsplatform.FailureRequeue,
})
if err != nil {
    return err
}
app := spine.New(spine.WithEventAdapter("nats", factory))
```

For HTTP applications, use `spine.New(echoplatform.Option(), spine.WithEventAdapter("nats", factory))`. Workers without HTTP do not select an HTTP platform. With a Spine version providing the new publishing API, publish with `publish.Event(ctx, destinations.OrderCreated, publish.Fields{"id": orderID})` and register consumers with `app.Consumers().Register("order.created", (*OrderConsumer).Handle)`.

When enabling publishing, configure a value implementing `PublishDestinations() []string`. During publisher initialization, the adapter checks that each prefixed subject belongs to the configured stream.

```go
// destinations.go
package destinations

const OrderCreated = "orders.created"

type Catalog struct{}

func (Catalog) PublishDestinations() []string {
    return []string{OrderCreated}
}
```

## Configuration

| Field | Default | Behavior and constraints |
| --- | --- | --- |
| `URLs` | Required | List of `nats://` or `tls://` addresses; credentials, paths, and queries in URLs are rejected |
| `Stream` | Required | Existing stream name; dots, whitespace, control characters, wildcards, and path separators are rejected |
| `Consume`, `Publish` | `false` | Enable at least one role |
| `PublishDestinations` | Required for publishing | Provides logical destination names |
| `ConsumerPrefix` | Required for consuming | Consumer service or group name; the same naming constraints as `Stream` apply |
| `TopicPrefix` | None | Literal subject prefix, such as `prod.`; a nonempty prefix must end with a dot |
| `FailurePolicy` | `FailureReject` | `FailureReject` sends TERM to stop redelivery; `FailureRequeue` sends NAK to request redelivery |
| `RequeueDelay` | `0` | Requeue delay; zero requests immediate redelivery |
| `AckWait` | `30s` | Server wait before redelivery when no ACK arrives |
| `MaxDeliver` | `-1` | Zero also means unlimited; positive values limit total server deliveries, including the first |
| `User`, `Password` | None | User authentication; a password cannot be configured alone |
| `Token` | None | Token authentication |
| `CredentialsFile` | None | NATS credentials file; select only one authentication method |
| `TLS` | TLS 1.2 or later | Copy of `tls.Config`, retaining default certificate verification |
| `AllowInsecureTransport` | `false` | Set true only for local plaintext connections; cannot be combined with TLS |
| `StartupTimeout` | `10s` | Connection and readiness deadline |
| `OperationTimeout` | `5s` | Publishing, ACK, and NACK deadline |

Negative durations are rejected. Subscription topics and event names must be nonempty literal NATS subjects; `*`, `>`, empty tokens, and whitespace are rejected. `TopicPrefix: "prod."` maps logical `order.created` to `prod.order.created`. Do not repeat the prefix in consumer registration.

## Delivery and shutdown

- Publishing waits for the selected stream's storage acknowledgement. Successful processing uses `DoubleAck` to wait for server confirmation. NACK sends TERM or NAK and waits for the connection to flush.
- Consumer names combine `ConsumerPrefix` with the first 128 bits of the mapped subject's SHA-256. Readers using the same stream, prefix, and topic share a durable consumer. Different prefixes have independent positions.
- `NewReader` prepares a consumer without fetching messages. The first `Read` starts pulling, one message at a time. `MaxAckPending` is 1 across the consumer. Call ACK or NACK once after the handler and post-processing.
- `Close` blocks new work and waits for in-flight operations. Closing a reader releases a pending `Read`. Cleanup continues after a wait context expires; a later `Close` can wait again. Durable consumers and streams are not deleted.
- Native automatic reconnection is disabled. Spine's shared recreation policy handles connection failures. Failed publish messages are not automatically resent.
- Time and metadata use the `Spine-Occurred-At` and JSON `Spine-Metadata` headers; payloads are not serialized again. Malformed headers from external producers are ignored. Missing or invalid timestamps fall back to server storage time. Received event names use the registered logical topic.

This implementation supports **JetStream only**. It does not implement Core NATS pub/sub, request/reply, wildcard consumption, or automatic DLQ setup. Parallel readers and redelivery can change ordering. Lost ACKs or processing beyond `AckWait` can cause duplicate execution; make consumers idempotent. No automatic `InProgress` heartbeat is sent. Reaching `MaxDeliver` or sending TERM does not automatically move a message to a DLQ; retention follows stream policy. Database commits and broker publishing are not atomic, and exactly-once business processing is not guaranteed.

## Local example and verification

Run from the repository root. Install the `nats` CLI using its [official instructions](https://github.com/nats-io/natscli#installation).

```sh
docker run --rm --name spine-nats -p 127.0.0.1:4222:4222 nats:2.12.1-alpine -js
```

In another terminal, create the stream and start the worker.

```sh
nats --server nats://127.0.0.1:4222 stream add EVENTS \
  --subjects 'order.>' --storage file --retention limits --defaults
NATS_URL=nats://127.0.0.1:4222 NATS_STREAM=EVENTS NATS_ALLOW_INSECURE=true \
  go -C examples/worker run .
```

Send `nats --server nats://127.0.0.1:4222 pub order.created '{"id":42}'`. The worker prints its order receipt message with `id=42`. Stop it with `Ctrl+C`.

```sh
go test -race ./... -count=1
bash scripts/check-module-boundary.sh
SPINE_TEST_NATS_URL=nats://127.0.0.1:4222 go test -race ./... -count=1
SPINE_TEST_NATS_URL=nats://127.0.0.1:4222 go -C e2e test -race ./... -count=1
```

Real server checks are skipped when environment variables are absent. Configure a separate TLS test server with `SPINE_TEST_NATS_TLS_URL`, `SPINE_TEST_NATS_TLS_CA`, `SPINE_TEST_NATS_USER`, and `SPINE_TEST_NATS_PASSWORD`. CI includes a local CA and a server with user authentication, and tests authentication failure and untrusted certificates.

E2E covers HTTP requests, domain event publishing, worker failure, redelivery, result events, and graceful shutdown. It uses one local server; cluster recovery, production certificates and accounts, throughput, and large-payload performance are outside its scope.

CI's `generated-project` job runs `spine new --event-platform events:nats` with a pinned CLI commit. It exposes this checkout through a separate module proxy and cache, without adding `replace` to the generated `go.mod`. Across two layouts, four HTTP application and worker combinations check static contracts, partial runtime probes, actual NATS delivery, ACK and invalid DTO rejection, graceful shutdown, and source-preserving updates.

Four additional combinations connect event names not registered in the CLI and configurable validators through metadata. These include different constructors and configuration structs, and indirect validator dependencies. Missing or skipped checks in any of the eight combinations fail validation. This verifies the current checkout, rather than installation of public modules.

See [NATS JetStream documentation](https://docs.nats.io/learn/jetstream/) and the [official Go client](https://github.com/nats-io/nats.go/tree/v1.55.0/jetstream) for delivery guarantees and pull consumers.

## Releases and public contracts

This platform publishes its own versions and release channels. The Spine version in `go.mod` is the minimum dependency for its public contracts and is managed separately from the core release channel. CI and public installation checks verify the platform against this module's dependencies. Core releases verify combinations of a new core and existing platform tags.

CLI-generated application combination checks are optional and do not determine whether this platform can publish. The platform's own JetStream checks remain required.

The next publication target is `v0.2.0`.
