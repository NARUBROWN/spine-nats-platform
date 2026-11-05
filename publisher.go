package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	event "github.com/NARUBROWN/spine/contract/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	metadataHeader = "Spine-Metadata"
	occurredHeader = "Spine-Occurred-At"
)

type publisher struct {
	*resource
	js     jetstream.JetStream
	config Config
}

var _ event.Publisher = (*publisher)(nil)

func (p *publisher) Publish(ctx context.Context, message event.Message) error {
	if err := p.begin(); err != nil {
		return err
	}
	defer p.active.Done()
	if !validSubject(message.EventName) {
		return fmt.Errorf("%w: event name must be a literal subject", event.ErrInvalidAdapter)
	}
	ctx, cancel := context.WithTimeout(ctx, p.config.OperationTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	msg := &nats.Msg{Subject: p.config.TopicPrefix + message.EventName, Data: message.Payload, Header: nats.Header{}}
	if !message.OccurredAt.IsZero() {
		msg.Header.Set(occurredHeader, message.OccurredAt.Format(time.RFC3339Nano))
	}
	if len(message.Metadata) > 0 {
		data, err := json.Marshal(message.Metadata)
		if err != nil {
			return err
		}
		msg.Header.Set(metadataHeader, string(data))
	}
	_, err := p.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(p.config.Stream))
	return err
}

func (p *publisher) Close(ctx context.Context) error { return p.close(ctx, nil) }
