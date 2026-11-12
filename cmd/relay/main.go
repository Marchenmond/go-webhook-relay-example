// Command relay receives signed webhooks and forwards them to a target URL.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Marchenmond/go-webhook-relay-example/relay"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	secret := os.Getenv("WEBHOOK_SECRET")
	target := os.Getenv("TARGET_URL")
	if secret == "" || target == "" {
		log.Error("WEBHOOK_SECRET and TARGET_URL are required")
		os.Exit(1)
	}

	q := relay.NewQueue(relay.QueueConfig{
		TargetURL:   target,
		Size:        envInt("QUEUE_SIZE", 100),
		Workers:     envInt("WORKERS", 2),
		MaxAttempts: envInt("MAX_ATTEMPTS", 5),
		BaseDelay:   time.Duration(envInt("BASE_DELAY_MS", 1000)) * time.Millisecond,
		MaxDelay:    time.Duration(envInt("MAX_DELAY_MS", 30000)) * time.Millisecond,
		Logger:      log,
	})
	h := &relay.Handler{
		Secret:       []byte(secret),
		MaxBodyBytes: int64(envInt("MAX_BODY_BYTES", 1<<20)),
		Queue:        q,
		Logger:       log,
	}
	srv := &http.Server{
		Addr:              envStr("ADDR", ":8080"),
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Error("http shutdown", "error", err)
	}
	if err := q.Close(sctx); err != nil {
		log.Error("queue did not drain in time", "error", err)
	}
	log.Info("bye")
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
