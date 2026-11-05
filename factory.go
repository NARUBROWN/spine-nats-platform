// Package platform은 Spine 이벤트 계약의 NATS JetStream 구현입니다.
package platform

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	event "github.com/NARUBROWN/spine/contract/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type FailurePolicy string

const (
	FailureReject  FailurePolicy = "reject"
	FailureRequeue FailurePolicy = "requeue"
)

// Config는 미리 생성한 stream의 송수신과 durable consumer 정책을 설정합니다.
// ConsumerPrefix가 같으면 같은 논리 topic의 reader들이 하나의 consumer를 공유합니다.
type Config struct {
	URLs                   []string
	Stream                 string
	Consume                bool
	Publish                bool
	ConsumerPrefix         string
	TopicPrefix            string
	FailurePolicy          FailurePolicy
	RequeueDelay           time.Duration
	AckWait                time.Duration
	MaxDeliver             int
	User                   string
	Password               string
	Token                  string
	CredentialsFile        string
	TLS                    *tls.Config
	AllowInsecureTransport bool
	StartupTimeout         time.Duration
	OperationTimeout       time.Duration
}

type factory struct{ config Config }

// NewFactory는 설정을 복사하고 검사하며 네트워크에 연결하지 않습니다.
func NewFactory(config Config) (event.Factory, error) {
	invalid := func(reason string) (event.Factory, error) {
		return nil, fmt.Errorf("%w: %s", event.ErrInvalidAdapter, reason)
	}
	if len(config.URLs) == 0 || !validName(config.Stream) || (!config.Consume && !config.Publish) {
		return invalid("URLs, stream and at least one role are required")
	}
	if config.Consume && !validName(config.ConsumerPrefix) {
		return invalid("Consume requires a consumer prefix without dots, wildcards, whitespace or path separators")
	}
	if config.TopicPrefix != "" && (!strings.HasSuffix(config.TopicPrefix, ".") || !validSubject(strings.TrimSuffix(config.TopicPrefix, "."))) {
		return invalid("topic prefix must contain literal subject tokens and end in a dot")
	}
	if config.StartupTimeout < 0 || config.OperationTimeout < 0 || config.AckWait < 0 || config.RequeueDelay < 0 || config.MaxDeliver < -1 {
		return invalid("timeouts, requeue delay and max deliveries are invalid")
	}
	if config.FailurePolicy == "" {
		config.FailurePolicy = FailureReject
	}
	if config.FailurePolicy != FailureReject && config.FailurePolicy != FailureRequeue {
		return invalid("failure policy must be reject or requeue")
	}
	authModes := 0
	for _, enabled := range []bool{config.User != "" || config.Password != "", config.Token != "", config.CredentialsFile != ""} {
		if enabled {
			authModes++
		}
	}
	if authModes > 1 || (config.Password != "" && config.User == "") {
		return invalid("select only one authentication method and supply a user with a password")
	}
	for _, address := range config.URLs {
		u, err := url.Parse(address)
		if err != nil || u.Hostname() == "" || (u.Scheme != "nats" && u.Scheme != "tls") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || strings.Contains(address, ",") {
			return invalid("URLs must be nats:// or tls:// server addresses; use separate authentication fields")
		}
		if config.AllowInsecureTransport {
			if u.Scheme != "nats" || config.TLS != nil {
				return invalid("plaintext opt-in cannot be combined with TLS")
			}
		} else if u.Scheme != "tls" && config.TLS == nil {
			return invalid("configure TLS or explicitly allow plaintext transport")
		}
	}
	config.URLs = append([]string(nil), config.URLs...)
	if config.TLS != nil {
		config.TLS = config.TLS.Clone()
	} else if !config.AllowInsecureTransport {
		config.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 10 * time.Second
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 5 * time.Second
	}
	if config.AckWait == 0 {
		config.AckWait = 30 * time.Second
	}
	if config.MaxDeliver == 0 {
		config.MaxDeliver = -1
	}
	return &factory{config: config}, nil
}

func validName(value string) bool {
	return value != "" && !strings.ContainsAny(value, ".*>/\\") && strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}

func validSubject(value string) bool {
	if value == "" || strings.ContainsAny(value, "*>") || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return false
	}
	for _, token := range strings.Split(value, ".") {
		if token == "" {
			return false
		}
	}
	return true
}

func (f *factory) Capabilities() event.Capabilities {
	return event.Capabilities{ContractVersion: event.ContractVersion, Consume: f.config.Consume, Publish: f.config.Publish}
}

func (f *factory) consumerName(topic string) string {
	// 논리 topic을 해시해 NATS 이름 제약과 구분자 치환의 충돌을 피합니다.
	hash := sha256.Sum256([]byte(f.config.TopicPrefix + topic))
	return fmt.Sprintf("%s-%x", f.config.ConsumerPrefix, hash[:16])
}

func (f *factory) NewReader(ctx context.Context, subscription event.Subscription) (event.Reader, error) {
	if !f.config.Consume {
		return nil, event.ErrUnsupportedCapability
	}
	if !validSubject(subscription.Topic) {
		return nil, fmt.Errorf("%w: subscription must be a literal subject", event.ErrInvalidAdapter)
	}
	ctx, cancel := context.WithTimeout(ctx, f.config.StartupTimeout)
	defer cancel()
	nc, js, err := f.connect(ctx)
	if err != nil {
		return nil, err
	}
	// CreateConsumer는 일치하는 기존 consumer를 재사용하며 다른 설정을 덮어쓰지 않습니다.
	c, err := js.CreateConsumer(ctx, f.config.Stream, jetstream.ConsumerConfig{
		Durable: f.consumerName(subscription.Topic), FilterSubject: f.config.TopicPrefix + subscription.Topic,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: f.config.AckWait,
		MaxDeliver: f.config.MaxDeliver, MaxAckPending: 1,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("prepare NATS consumer: %w", err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	r := &reader{consumer: c, nc: nc, config: f.config, topic: subscription.Topic, ctx: runCtx, stop: stop}
	r.resource = newResource(func() {
		if r.iterator != nil {
			r.iterator.Stop()
		}
		nc.Close()
	})
	return r, nil
}

func (f *factory) NewPublisher(ctx context.Context) (event.Publisher, error) {
	if !f.config.Publish {
		return nil, event.ErrUnsupportedCapability
	}
	ctx, cancel := context.WithTimeout(ctx, f.config.StartupTimeout)
	defer cancel()
	nc, js, err := f.connect(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = js.Stream(ctx, f.config.Stream); err != nil {
		nc.Close()
		return nil, fmt.Errorf("prepare NATS stream: %w", err)
	}
	return &publisher{resource: newResource(nc.Close), js: js, config: f.config}, nil
}

// startupDialer는 TCP, TLS와 NATS handshake 도중에도 생성 취소를 반영합니다.
type startupDialer struct {
	ctx    context.Context
	mu     sync.Mutex
	closed bool
	wires  []net.Conn
}

func (d *startupDialer) Dial(network, address string) (net.Conn, error) {
	wire, err := (&net.Dialer{}).DialContext(d.ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.ctx.Err() != nil {
		wire.Close()
		return nil, context.Canceled
	}
	if deadline, ok := d.ctx.Deadline(); ok {
		if err := wire.SetDeadline(deadline); err != nil {
			wire.Close()
			return nil, err
		}
	}
	d.wires = append(d.wires, wire)
	return wire, nil
}

func (d *startupDialer) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, wire := range d.wires {
		wire.Close()
	}
}

func (f *factory) connect(ctx context.Context) (*nats.Conn, jetstream.JetStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	dialer := &startupDialer{ctx: ctx}
	stopped := make(chan struct{})
	unwatch := context.AfterFunc(ctx, func() { dialer.close(); close(stopped) })
	options := []nats.Option{nats.NoReconnect(), nats.Timeout(f.config.StartupTimeout), nats.FlusherTimeout(f.config.OperationTimeout), nats.SetCustomDialer(dialer)}
	if f.config.TLS != nil {
		options = append(options, nats.Secure(f.config.TLS.Clone()))
	}
	if f.config.User != "" {
		options = append(options, nats.UserInfo(f.config.User, f.config.Password))
	}
	if f.config.Token != "" {
		options = append(options, nats.Token(f.config.Token))
	}
	if f.config.CredentialsFile != "" {
		options = append(options, nats.UserCredentials(f.config.CredentialsFile))
	}
	nc, err := nats.Connect(strings.Join(f.config.URLs, ","), options...)
	if !unwatch() {
		<-stopped
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		dialer.close()
		if nc != nil {
			nc.Close()
		}
		return nil, nil, fmt.Errorf("connect NATS: %w", err)
	}
	for _, wire := range dialer.wires {
		// 초기화 제한 시간은 연결 생성에만 적용합니다.
		_ = wire.SetDeadline(time.Time{})
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}
