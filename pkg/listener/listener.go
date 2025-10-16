package listener

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	errListenerStopped = errors.New("listener stopped")
)

type Job interface {
	Start(ctx context.Context)
	Stop()
}

type Listener[T any] struct {
	handler     func(input T) error
	stopHandler func()

	in     <-chan T
	wg     sync.WaitGroup
	cancel func()
}

func New[T any](
	in <-chan T,
	handler func(T) error,
	stopHandler ...func(),
) *Listener[T] {
	if len(stopHandler) == 0 {
		stopHandler = []func(){func() {}}
	}

	return &Listener[T]{
		in:          in,
		handler:     handler,
		cancel:      func() {},
		stopHandler: stopHandler[0],
	}
}

func (l *Listener[T]) Start(ctx context.Context) {
	ctx, l.cancel = context.WithCancel(ctx)
	l.wg.Add(1)

	go func() {
		defer l.wg.Done()
		for {
			err := l.run(ctx)
			switch {
			case errors.Is(err, errListenerStopped):
				return
			case err != nil:
				panic("channel listener error: " + err.Error())
			}
		}
	}()
}

func (l *Listener[T]) run(ctx context.Context) error {
	select {
	case inp := <-l.in:
		err := l.handler(inp)
		if err != nil {
			return fmt.Errorf("failed to handle input: %w", err)
		}
	case <-ctx.Done():
		return errListenerStopped
	}

	return nil
}

func (l *Listener[T]) Stop() {
	l.cancel()
	l.wg.Wait()
	l.stopHandler()
}
