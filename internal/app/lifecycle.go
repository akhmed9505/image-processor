// Package app assembles the server and worker and coordinates their lifetimes.
package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	delivery "github.com/akhmed9505/image-processor/internal/delivery/kafka"
)

// ErrShutdownIncomplete requires process termination without closing shared dependencies.
var ErrShutdownIncomplete = errors.New("shutdown incomplete; shared dependencies remain open")

// supervise bounds shutdown, including the consumer's forced-cancellation join.
func supervise(ctx context.Context, components ...func(context.Context) error) (bool, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(components))
	for _, run := range components {
		go func() { results <- run(runCtx) }()
	}
	remaining := len(components)
	var failure error
	select {
	case <-ctx.Done():
	case err := <-results:
		remaining--
		if err == nil && ctx.Err() == nil {
			err = errors.New("background component stopped unexpectedly")
		}
		if err != context.Canceled {
			failure = err
		}
	}
	cancel()
	timer := time.NewTimer(40 * time.Second)
	defer timer.Stop()
	pending := []<-chan struct{}{}
	collect := func(err error) {
		var shutdown *delivery.ShutdownError
		if errors.As(err, &shutdown) {
			pending = append(pending, shutdown.Done)
		}
		var httpShutdown *httpShutdownError
		if errors.As(err, &httpShutdown) {
			pending = append(pending, httpShutdown.done)
		}
		if err != nil && err != context.Canceled {
			failure = errors.Join(failure, err)
		}
	}
	// The first failing component may itself report outstanding workers.
	var first *delivery.ShutdownError
	if errors.As(failure, &first) {
		pending = append(pending, first.Done)
	}
	var firstHTTP *httpShutdownError
	if errors.As(failure, &firstHTTP) {
		pending = append(pending, firstHTTP.done)
	}
	for remaining > 0 {
		select {
		case err := <-results:
			remaining--
			collect(err)
		case <-timer.C:
			return false, errors.Join(failure, ErrShutdownIncomplete)
		}
	}
	for _, done := range pending {
		select {
		case <-done:
		case <-timer.C:
			return false, errors.Join(failure, ErrShutdownIncomplete)
		}
	}
	return true, failure
}

type closeStack []func()

func (c closeStack) close() {
	for i := len(c) - 1; i >= 0; i-- {
		c[i]()
	}
}
func stage(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", name, err)
}

func (c closeStack) closeBounded() error {
	done := make(chan struct{})
	go func() { c.close(); close(done) }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("%w: dependency close timed out", ErrShutdownIncomplete)
	}
}
