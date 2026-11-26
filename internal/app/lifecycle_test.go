package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
	delivery "github.com/akhmed9505/image-processor/internal/delivery/kafka"
)

func TestSuperviseCancellationAndFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		sentinel := errors.New("background failure")
		finished := make(chan struct{})
		safe, err := supervise(ctx, func(c context.Context) error { <-c.Done(); close(finished); return nil }, func(c context.Context) error {
			if failed {
				return sentinel
			}
			cancel()
			return nil
		})
		if !safe || failed && !errors.Is(err, sentinel) || !failed && err != nil {
			t.Fatal(safe, err)
		}
		select {
		case <-finished:
		default:
			t.Fatal("dependent goroutine not joined")
		}
		cancel()
	}
}
func TestSuperviseWaitsForConsumerDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	returned := make(chan struct{})
	done := make(chan struct{})
	go func() {
		safe, err := supervise(ctx, func(context.Context) error { return &delivery.ShutdownError{Done: release} })
		if !safe || err == nil {
			t.Error(safe, err)
		}
		close(returned)
		close(done)
	}()
	select {
	case <-returned:
		t.Fatal("pending workers ignored")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not join completion")
	}
}
func TestResourceRollback(t *testing.T) {
	var order []int
	stack := closeStack{func() { order = append(order, 1) }, func() { order = append(order, 2) }}
	if e := stack.closeBounded(); e != nil || len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatal(order, e)
	}
}
func TestHTTPGracefulAndForcedShutdown(t *testing.T) {
	for _, forced := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		started := make(chan struct{})
		release := make(chan struct{})
		handlerDone := make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerDone)
			close(started)
			if forced {
				<-r.Context().Done()
			} else {
				<-release
			}
			w.WriteHeader(204)
		})
		cfg := config.HTTP{ShutdownTimeout: 30 * time.Millisecond, WriteTimeout: time.Second}
		done := make(chan error, 1)
		go func() { done <- serve(ctx, listener, cfg, handler, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
		clientDone := make(chan struct{})
		go func() {
			defer close(clientDone)
			r, e := http.Get("http://" + listener.Addr().String())
			if e == nil {
				r.Body.Close()
			}
		}()
		<-started
		cancel()
		if !forced {
			unblock()
		}
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(time.Second):
			unblock()
			t.Fatal("HTTP shutdown blocked")
		}
		select {
		case <-handlerDone:
		default:
			t.Fatal("HTTP dependency could close before handler finished")
		}
		<-clientDone
	}
}

func TestSuperviseBoundedPendingShutdown(t *testing.T) {
	t.Parallel()
	pending := make(chan struct{})
	defer close(pending)
	before := time.Now()
	safe, err := supervise(context.Background(), func(context.Context) error { return &delivery.ShutdownError{Done: pending} })
	if safe || !errors.Is(err, ErrShutdownIncomplete) || time.Since(before) > 42*time.Second {
		t.Fatal("unbounded or unsafe shutdown", safe, err)
	}
}

func TestSuperviseHTTPPendingShutdown(t *testing.T) {
	pending := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		safe, err := supervise(context.Background(), func(context.Context) error { return &httpShutdownError{pending} })
		if !safe || err == nil {
			t.Error(safe, err)
		}
		close(finished)
	}()
	select {
	case <-finished:
		t.Fatal("HTTP handlers were not joined")
	case <-time.After(20 * time.Millisecond):
	}
	close(pending)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("HTTP completion ignored")
	}
}
