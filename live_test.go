package platform

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	event "github.com/NARUBROWN/spine/contract/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func liveConfig(t *testing.T) (Config, jetstream.JetStream) {
	t.Helper()
	address := os.Getenv("SPINE_TEST_NATS_URL")
	if address == "" {
		t.Skip("SPINE_TEST_NATS_URL로 실제 JetStream 서버를 지정하세요")
	}
	nc, err := nats.Connect(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig()
	config.URLs = []string{address}
	config.Stream = fmt.Sprintf("SPINE_%d", time.Now().UnixNano())
	config.TopicPrefix = config.Stream + "."
	config.StartupTimeout = 3 * time.Second
	config.OperationTimeout = time.Second
	config.AckWait = time.Second
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: config.Stream, Subjects: []string{config.TopicPrefix + ">"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := js.DeleteStream(ctx, config.Stream); err != nil {
			t.Error(err)
		}
	})
	return config, js
}

func liveFactory(t *testing.T, config Config) event.Factory {
	t.Helper()
	f, err := NewFactory(config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func liveReader(t *testing.T, f event.Factory, topic string) event.Reader {
	t.Helper()
	r, err := f.NewReader(t.Context(), event.Subscription{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

func livePublisher(t *testing.T, f event.Factory) event.Publisher {
	t.Helper()
	p, err := f.NewPublisher(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	return p
}

func readOne(t *testing.T, r event.Reader) event.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d, err := r.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestLiveRoundTripAndReadiness(t *testing.T) {
	config, _ := liveConfig(t)
	f := liveFactory(t, config)
	r := liveReader(t, f, "order.created")
	p := livePublisher(t, f)
	message := event.Message{EventName: "order.created", Payload: []byte("{\"id\":7}"), OccurredAt: time.Now().UTC(), Metadata: map[string]string{"trace": "hello\r\nworld", "한글": "값"}}
	if err := p.Publish(t.Context(), message); err != nil {
		t.Fatal(err)
	}
	info, err := r.(*reader).consumer.Info(t.Context())
	if err != nil || info.NumAckPending != 0 || info.NumWaiting != 0 {
		t.Fatalf("NewReader pulled messages before Read: %+v, %v", info, err)
	}
	d := readOne(t, r)
	got := d.Message()
	if got.EventName != message.EventName || !bytes.Equal(got.Payload, message.Payload) || !got.OccurredAt.Equal(message.OccurredAt) || !reflect.DeepEqual(got.Metadata, message.Metadata) {
		t.Fatalf("message mismatch: %+v", got)
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := d.Nack(t.Context()); !errors.Is(err, event.ErrInvalidState) {
		t.Fatalf("duplicate settlement: %v", err)
	}
	info, err = r.(*reader).consumer.Info(t.Context())
	if err != nil || info.NumAckPending != 0 {
		t.Fatalf("ack was not confirmed: %+v, %v", info, err)
	}
}

func TestLiveFailurePolicies(t *testing.T) {
	for _, policy := range []FailurePolicy{FailureReject, FailureRequeue} {
		t.Run(string(policy), func(t *testing.T) {
			config, _ := liveConfig(t)
			config.FailurePolicy, config.RequeueDelay = policy, 30*time.Millisecond
			f := liveFactory(t, config)
			r := liveReader(t, f, "order.created")
			p := livePublisher(t, f)
			if err := p.Publish(t.Context(), event.Message{EventName: "order.created", Payload: []byte("retry")}); err != nil {
				t.Fatal(err)
			}
			d := readOne(t, r)
			if err := d.Nack(t.Context()); err != nil {
				t.Fatal(err)
			}
			if policy == FailureRequeue {
				redelivered := readOne(t, r)
				meta, err := redelivered.(*delivery).native.Metadata()
				if err != nil || meta.NumDelivered < 2 || string(redelivered.Message().Payload) != "retry" {
					t.Fatalf("no redelivery: %+v, %v", meta, err)
				}
				if err := redelivered.Ack(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if _, err := r.Read(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected further delivery: %v", err)
			}
		})
	}
}

func TestLiveDurableRestartAndConfigurationConflict(t *testing.T) {
	config, _ := liveConfig(t)
	f := liveFactory(t, config)
	r := liveReader(t, f, "order.created")
	p := livePublisher(t, f)
	for _, payload := range []string{"one", "two"} {
		if err := p.Publish(t.Context(), event.Message{EventName: "order.created", Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	first := readOne(t, r)
	if err := first.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r2 := liveReader(t, f, "order.created")
	second := readOne(t, r2)
	if string(first.Message().Payload) != "one" || string(second.Message().Payload) != "two" {
		t.Fatal("durable position was lost")
	}
	if err := second.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	config.AckWait *= 2
	conflict := liveFactory(t, config)
	if reader, err := conflict.NewReader(t.Context(), event.Subscription{Topic: "order.created"}); err == nil || reader != nil {
		t.Fatal("consumer configuration was silently overwritten")
	}
}

func TestLiveSharedConsumerAndIndependentGroup(t *testing.T) {
	config, _ := liveConfig(t)
	f := liveFactory(t, config)
	first := liveReader(t, f, "order.created")
	second := liveReader(t, f, "order.created")
	if first.(*reader).consumer.CachedInfo().Name != second.(*reader).consumer.CachedInfo().Name {
		t.Fatal("readers did not share a durable consumer")
	}
	otherConfig := config
	otherConfig.ConsumerPrefix = "independent"
	other := liveReader(t, liveFactory(t, otherConfig), "order.created")
	p := livePublisher(t, f)
	for _, payload := range []string{"one", "two"} {
		if err := p.Publish(t.Context(), event.Message{EventName: "order.created", Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	for i, r := range []event.Reader{first, second} {
		d := readOne(t, r)
		if string(d.Message().Payload) != []string{"one", "two"}[i] {
			t.Fatal("shared consumer lost its position")
		}
		if err := d.Ack(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	independent := readOne(t, other)
	if string(independent.Message().Payload) != "one" {
		t.Fatal("independent group did not start from its own position")
	}
	if err := independent.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestLiveUnsettledRedeliveryAndMaxDeliver(t *testing.T) {
	config, _ := liveConfig(t)
	config.MaxDeliver, config.AckWait = 2, 100*time.Millisecond
	config.FailurePolicy = FailureRequeue
	f := liveFactory(t, config)
	r := liveReader(t, f, "order.created")
	p := livePublisher(t, f)
	if err := p.Publish(t.Context(), event.Message{EventName: "order.created", Payload: []byte("unsettled")}); err != nil {
		t.Fatal(err)
	}
	_ = readOne(t, r)
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r = liveReader(t, f, "order.created")
	d := readOne(t, r)
	meta, err := d.(*delivery).native.Metadata()
	if err != nil || meta.NumDelivered != 2 {
		t.Fatalf("unsettled message was not redelivered: %+v, %v", meta, err)
	}
	if err := d.Nack(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := r.Read(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("MaxDeliver was not enforced: %v", err)
	}
}

func TestLiveForeignHeadersCannotChangeSubscription(t *testing.T) {
	config, _ := liveConfig(t)
	f := liveFactory(t, config)
	r := liveReader(t, f, "order.created")
	nc, err := nats.Connect(config.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.PublishMsg(t.Context(), &nats.Msg{
		Subject: config.TopicPrefix + "order.created", Data: []byte("foreign"),
		Header: nats.Header{"Spine-Event-Name": {"admin.delete"}, metadataHeader: {"broken JSON"}, occurredHeader: {"broken time"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := readOne(t, r)
	if got := d.Message(); got.EventName != "order.created" || got.OccurredAt.IsZero() || got.Metadata != nil || string(got.Payload) != "foreign" {
		t.Fatalf("foreign headers changed delivery: %+v", got)
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestLiveReadCancellationAndClose(t *testing.T) {
	config, _ := liveConfig(t)
	f := liveFactory(t, config)
	r := liveReader(t, f, "order.created")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := r.Read(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	p := livePublisher(t, f)
	if err := p.Publish(t.Context(), event.Message{EventName: "order.created"}); err != nil {
		t.Fatal(err)
	}
	// 취소된 읽기의 pull이 다음 메시지를 가로채지 않는지 확인합니다.
	d := readOne(t, r)
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := r.Read(context.Background()); done <- err }()
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, event.ErrInvalidState) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Read")
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(t.Context()); !errors.Is(err, event.ErrInvalidState) {
		t.Fatal(err)
	}
}

func TestLiveConcurrentPublishAndClose(t *testing.T) {
	config, js := liveConfig(t)
	p := livePublisher(t, liveFactory(t, config))
	var workers sync.WaitGroup
	for range 40 {
		workers.Go(func() {
			if err := p.Publish(t.Context(), event.Message{EventName: "order.created", Payload: []byte("concurrent")}); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	stream, err := js.Stream(t.Context(), config.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if stream.CachedInfo().State.Msgs != 40 {
		t.Fatal("concurrent publication lost messages")
	}
	for range 40 {
		workers.Go(func() {
			err := p.Publish(t.Context(), event.Message{EventName: "order.created", Payload: []byte("concurrent")})
			if err != nil && !errors.Is(err, event.ErrInvalidState) {
				t.Error(err)
			}
		})
	}
	if err := p.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	if err := p.Publish(t.Context(), event.Message{EventName: "order.created"}); !errors.Is(err, event.ErrInvalidState) {
		t.Fatal(err)
	}
}

func TestLiveMissingStreamAndWrongPublicationTarget(t *testing.T) {
	config, _ := liveConfig(t)
	f := liveFactory(t, config)
	p := livePublisher(t, f)
	if err := p.Publish(t.Context(), event.Message{EventName: "order.*"}); !errors.Is(err, event.ErrInvalidAdapter) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.Publish(ctx, event.Message{EventName: "order.created"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	other := config
	other.TopicPrefix = "elsewhere."
	if err := livePublisher(t, liveFactory(t, other)).Publish(t.Context(), event.Message{EventName: "order.created"}); err == nil {
		t.Fatal("publication to missing stream subject succeeded")
	}
	config.Stream += "_MISSING"
	missing := liveFactory(t, config)
	if p, err := missing.NewPublisher(t.Context()); err == nil || p != nil {
		t.Fatal("publisher ignored missing stream")
	}
	if r, err := missing.NewReader(t.Context(), event.Subscription{Topic: "order.created"}); err == nil || r != nil {
		t.Fatal("reader ignored missing stream")
	}
}

func TestLiveTLSAndAuthentication(t *testing.T) {
	address, certPath := os.Getenv("SPINE_TEST_NATS_TLS_URL"), os.Getenv("SPINE_TEST_NATS_TLS_CA")
	if address == "" || certPath == "" {
		t.Skip("TLS 서버 주소와 테스트 CA가 필요합니다")
	}
	data, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		t.Fatal("테스트 CA를 읽지 못했습니다")
	}
	tlsConfig := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	user, password := os.Getenv("SPINE_TEST_NATS_USER"), os.Getenv("SPINE_TEST_NATS_PASSWORD")
	nc, err := nats.Connect(address, nats.Secure(tlsConfig), nats.UserInfo(user, password))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream := fmt.Sprintf("SPINE_TLS_%d", time.Now().UnixNano())
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: stream, Subjects: []string{stream + ".>"}}); err != nil {
		t.Fatal(err)
	}
	defer js.DeleteStream(context.Background(), stream)
	config := Config{URLs: []string{address}, Stream: stream, TopicPrefix: stream + ".", ConsumerPrefix: "tls", Consume: true, Publish: true, TLS: tlsConfig, User: user, Password: password}
	f := liveFactory(t, config)
	r, p := liveReader(t, f, "order.created"), livePublisher(t, f)
	if err := p.Publish(t.Context(), event.Message{EventName: "order.created", Payload: []byte("authenticated")}); err != nil {
		t.Fatal(err)
	}
	d := readOne(t, r)
	if string(d.Message().Payload) != "authenticated" {
		t.Fatal("TLS round trip failed")
	}
	if err := d.Ack(t.Context()); err != nil {
		t.Fatal(err)
	}
	config.Password = "incorrect-password"
	if p, err := liveFactory(t, config).NewPublisher(t.Context()); err == nil || p != nil {
		t.Fatal("invalid credentials were accepted")
	}
	config.Password, config.TLS = password, nil
	if p, err := liveFactory(t, config).NewPublisher(t.Context()); err == nil || p != nil {
		t.Fatal("untrusted certificate was accepted")
	}
}
