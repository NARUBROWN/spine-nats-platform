package platform

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	event "github.com/NARUBROWN/spine/contract/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type reader struct {
	*resource
	consumer jetstream.Consumer
	iterator jetstream.MessagesContext
	nc       *nats.Conn
	config   Config
	topic    string
	ctx      context.Context
	stop     context.CancelFunc
}

var _ event.Reader = (*reader)(nil)

func (r *reader) Read(ctx context.Context) (event.Delivery, error) {
	if err := r.begin(); err != nil {
		return nil, err
	}
	defer r.active.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	unwatch := context.AfterFunc(r.ctx, cancel)
	defer unwatch()
	if r.ctx.Err() != nil {
		return nil, event.ErrInvalidState
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.iterator == nil {
		iterator, err := r.consumer.Messages(jetstream.PullMaxMessages(1))
		if err != nil {
			return nil, err
		}
		r.iterator = iterator
	}
	msg, err := r.iterator.Next(jetstream.NextContext(ctx))
	if r.ctx.Err() != nil {
		return nil, event.ErrInvalidState
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	// 수신 경로는 등록한 topic으로 고정하며 생산자 헤더로 선택하지 않습니다.
	message := event.Message{EventName: r.topic, Payload: msg.Data()}
	if meta, err := msg.Metadata(); err == nil {
		message.OccurredAt = meta.Timestamp
	}
	if timestamp, err := time.Parse(time.RFC3339Nano, msg.Headers().Get(occurredHeader)); err == nil {
		message.OccurredAt = timestamp
	}
	var metadata map[string]string
	if json.Unmarshal([]byte(msg.Headers().Get(metadataHeader)), &metadata) == nil {
		message.Metadata = metadata
	}
	return &delivery{reader: r, native: msg, message: message}, nil
}

func (r *reader) Close(ctx context.Context) error { return r.close(ctx, r.stop) }

type delivery struct {
	reader  *reader
	native  jetstream.Msg
	message event.Message
	settled atomic.Bool
}

func (d *delivery) Message() event.Message { return d.message }

func (d *delivery) Ack(ctx context.Context) error { return d.settle(ctx, true) }

func (d *delivery) Nack(ctx context.Context) error { return d.settle(ctx, false) }

func (d *delivery) settle(ctx context.Context, success bool) error {
	if err := d.reader.begin(); err != nil {
		return err
	}
	defer d.reader.active.Done()
	if !d.settled.CompareAndSwap(false, true) {
		return event.ErrInvalidState
	}
	ctx, cancel := context.WithTimeout(ctx, d.reader.config.OperationTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if success {
		return d.native.DoubleAck(ctx)
	}
	var err error
	if d.reader.config.FailurePolicy == FailureRequeue {
		err = d.native.NakWithDelay(d.reader.config.RequeueDelay)
	} else {
		err = d.native.Term()
	}
	if err != nil {
		return err
	}
	return d.reader.nc.FlushWithContext(ctx)
}
