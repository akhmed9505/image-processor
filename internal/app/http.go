package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
)

type httpShutdownError struct{ done <-chan struct{} }

func (*httpShutdownError) Error() string { return "HTTP handlers did not stop after cancellation" }

func serve(ctx context.Context, listener net.Listener, cfg config.HTTP, handler http.Handler, log *slog.Logger) error {
	requests, cancelRequests := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRequests()
	var mu sync.Mutex
	active := 0
	closing := false
	done := make(chan struct{})
	tracked := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if closing {
			mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		active++
		mu.Unlock()
		defer func() {
			mu.Lock()
			active--
			if closing && active == 0 {
				close(done)
			}
			mu.Unlock()
		}()
		requestCtx, stopRequest := context.WithTimeout(r.Context(), cfg.WriteTimeout)
		defer stopRequest()
		handler.ServeHTTP(w, r.WithContext(requestCtx))
	})
	server := &http.Server{Handler: tracked, ReadHeaderTimeout: cfg.ReadHeaderTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout, IdleTimeout: cfg.IdleTimeout, BaseContext: func(net.Listener) context.Context { return requests }}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	var failure error
	select {
	case <-ctx.Done():
	case err := <-stopped:
		if !errors.Is(err, http.ErrServerClosed) {
			failure = err
		}
		stopped = nil
	}
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	err := server.Shutdown(shutdown)
	cancel()
	mu.Lock()
	closing = true
	if active == 0 {
		close(done)
	}
	mu.Unlock()
	cancelRequests()
	if err != nil {
		log.Warn("HTTP graceful shutdown deadline exceeded")
		_ = server.Close()
	}
	if stopped != nil {
		<-stopped
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
		return failure
	case <-timer.C:
		return errors.Join(failure, &httpShutdownError{done})
	}
}
