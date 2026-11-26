package app

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akhmed9505/image-processor/internal/config"
)

// Main runs a signal-aware entry point and enforces a final process shutdown deadline.
func Main(worker bool) int {
	path := flag.String("config", "configs/config.yml", "configuration path")
	migrate := flag.Bool("migrate", false, "apply migrations and exit (server only)")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Configuration could not be loaded:", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// This guard terminates the process without racing explicit dependency closes.
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			timer := time.NewTimer(50 * time.Second)
			defer timer.Stop()
			select {
			case <-finished:
			case <-timer.C:
				fmt.Fprintln(os.Stderr, "Process shutdown deadline exceeded")
				os.Exit(1)
			}
		case <-finished:
		}
	}()
	if *migrate {
		if worker {
			fmt.Fprintln(os.Stderr, "Use server -migrate")
			return 1
		}
		err = Migrate(ctx, cfg)
	} else if worker {
		err = RunWorker(ctx, cfg, os.Stdout)
	} else {
		err = RunServer(ctx, cfg, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Application failed:", err)
		return 1
	}
	return 0
}
