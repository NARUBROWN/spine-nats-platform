package platform

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	event "github.com/NARUBROWN/spine/contract/event"
)

func testConfig() Config {
	return Config{URLs: []string{"nats://127.0.0.1:4222"}, Stream: "EVENTS", ConsumerPrefix: "orders", Consume: true, Publish: true, AllowInsecureTransport: true}
}

func TestFactoryValidation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Config)
	}{
		{"no URLs", func(c *Config) { c.URLs = nil }},
		{"bad URL", func(c *Config) { c.URLs[0] = "http://localhost:4222" }},
		{"URL credentials", func(c *Config) { c.URLs[0] = "nats://secret:secret@localhost:4222" }},
		{"URL list injection", func(c *Config) { c.URLs[0] = "nats://localhost:4222,nats://other:4222" }},
		{"URL path", func(c *Config) { c.URLs[0] += "/path" }},
		{"no stream", func(c *Config) { c.Stream = "" }},
		{"bad stream", func(c *Config) { c.Stream = "events.*" }},
		{"no roles", func(c *Config) { c.Consume = false; c.Publish = false }},
		{"no consumer prefix", func(c *Config) { c.ConsumerPrefix = "" }},
		{"bad consumer prefix", func(c *Config) { c.ConsumerPrefix = "orders.foo" }},
		{"wildcard prefix", func(c *Config) { c.TopicPrefix = "prod.*." }},
		{"prefix without separator", func(c *Config) { c.TopicPrefix = "prod" }},
		{"negative timeout", func(c *Config) { c.StartupTimeout = -1 }},
		{"negative ack wait", func(c *Config) { c.AckWait = -1 }},
		{"negative delay", func(c *Config) { c.RequeueDelay = -1 }},
		{"bad max deliver", func(c *Config) { c.MaxDeliver = -2 }},
		{"bad policy", func(c *Config) { c.FailurePolicy = "drop" }},
		{"plaintext default", func(c *Config) { c.AllowInsecureTransport = false }},
		{"ambiguous TLS", func(c *Config) { c.TLS = &tls.Config{} }},
		{"ambiguous auth", func(c *Config) { c.User = "user"; c.Token = "token" }},
		{"password without user", func(c *Config) { c.Password = "secret" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig()
			test.edit(&config)
			_, err := NewFactory(config)
			if !errors.Is(err, event.ErrInvalidAdapter) {
				t.Fatalf("want invalid adapter, got %v", err)
			}
		})
	}
}

func TestFactorySnapshotAndRoles(t *testing.T) {
	config := testConfig()
	config.AllowInsecureTransport = false
	config.TLS = &tls.Config{ServerName: "original"}
	f, err := NewFactory(config)
	if err != nil {
		t.Fatal(err)
	}
	config.URLs[0] = "nats://changed:4222"
	config.TLS.ServerName = "changed"
	snapshot := f.(*factory).config
	if snapshot.URLs[0] != "nats://127.0.0.1:4222" || snapshot.TLS.ServerName != "original" {
		t.Fatal("factory retained mutable config")
	}
	if err := event.ValidateFactory(f, event.Requirements{Consume: true, Publish: true}); err != nil {
		t.Fatal(err)
	}
	config = testConfig()
	config.Consume = false
	f, _ = NewFactory(config)
	if _, err := f.NewReader(context.Background(), event.Subscription{Topic: "order.created"}); !errors.Is(err, event.ErrUnsupportedCapability) {
		t.Fatal(err)
	}
	config.Consume, config.Publish = true, false
	f, _ = NewFactory(config)
	if _, err := f.NewPublisher(context.Background()); !errors.Is(err, event.ErrUnsupportedCapability) {
		t.Fatal(err)
	}
	for _, topic := range []string{"", "a.*", "a.>", "a..b", "a b", "a\r\nb"} {
		if _, err := f.NewReader(context.Background(), event.Subscription{Topic: topic}); !errors.Is(err, event.ErrInvalidAdapter) {
			t.Fatalf("topic %q: %v", topic, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.NewReader(ctx, event.Subscription{Topic: "order.created"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStartupCancellationDuringHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	config := testConfig()
	config.URLs = []string{"nats://" + listener.Addr().String()}
	f, _ := NewFactory(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.NewPublisher(ctx); done <- err }()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("connection was not established")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake ignored cancellation")
	}
}

func TestCloseWaitCanBeRetried(t *testing.T) {
	finished := make(chan struct{})
	r := newResource(func() { close(finished) })
	if err := r.begin(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.close(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.begin(); !errors.Is(err, event.ErrInvalidState) {
		t.Fatal(err)
	}
	r.active.Done()
	if err := r.close(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	<-finished
	if err := r.close(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}
