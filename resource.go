package platform

import (
	"context"
	"sync"

	event "github.com/NARUBROWN/spine/contract/event"
)

// resource는 새 호출을 차단한 뒤 진행 중인 작업과 정리를 한 번만 기다립니다.
type resource struct {
	mu     sync.Mutex
	closed bool
	active sync.WaitGroup
	done   chan struct{}
	finish func()
}

func newResource(finish func()) *resource {
	return &resource{done: make(chan struct{}), finish: finish}
}

func (r *resource) begin() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return event.ErrInvalidState
	}
	r.active.Add(1)
	return nil
}

func (r *resource) close(ctx context.Context, unblock func()) error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		if unblock != nil {
			unblock()
		}
		go func() {
			r.active.Wait()
			r.finish()
			close(r.done)
		}()
	}
	r.mu.Unlock()
	select {
	case <-r.done:
		return nil
	default:
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
