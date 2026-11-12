package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Event is one accepted webhook waiting to be delivered.
type Event struct {
	ID          string
	Source      string
	ContentType string
	Body        []byte
	ReceivedAt  time.Time
}

// ErrQueueFull is returned by Enqueue when the in-memory queue has no room.
var ErrQueueFull = errors.New("queue full")

// ErrClosed is returned by Enqueue after Close was called.
var ErrClosed = errors.New("queue closed")

// QueueConfig controls delivery.
type QueueConfig struct {
	TargetURL   string
	Size        int           // queue capacity
	Workers     int           // concurrent deliveries
	MaxAttempts int           // total tries per event, including the first
	BaseDelay   time.Duration // wait before the 2nd try; doubles each time
	MaxDelay    time.Duration // upper bound for the wait
	Client      *http.Client
	Logger      *slog.Logger
}

// Queue is an in-memory queue with workers that forward events to a target
// URL, retrying with exponential backoff. Events are lost if the process dies.
type Queue struct {
	cfg    QueueConfig
	ch     chan Event
	wg     sync.WaitGroup
	mu     sync.RWMutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
}

// NewQueue builds a queue and starts its workers.
func NewQueue(cfg QueueConfig) *Queue {
	if cfg.Size <= 0 {
		cfg.Size = 100
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * time.Second
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{cfg: cfg, ch: make(chan Event, cfg.Size), ctx: ctx, cancel: cancel}
	for i := 0; i < cfg.Workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	return q
}

// Enqueue adds an event without blocking.
func (q *Queue) Enqueue(e Event) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrClosed
	}
	select {
	case q.ch <- e:
		return nil
	default:
		return ErrQueueFull
	}
}

// Len is the number of events waiting (not counting those being delivered).
func (q *Queue) Len() int { return len(q.ch) }

// Close stops accepting events and waits for the workers to drain the queue.
// If ctx ends first, pending retries are abandoned and the remaining events
// are dropped (and logged by the workers).
func (q *Queue) Close(ctx context.Context) error {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
	q.mu.Unlock()

	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	select {
	case <-done:
		q.cancel()
		return nil
	case <-ctx.Done():
		q.cancel()
		<-done
		return ctx.Err()
	}
}

func (q *Queue) worker() {
	defer q.wg.Done()
	for e := range q.ch {
		q.deliver(e)
	}
}

func (q *Queue) deliver(e Event) {
	log := q.cfg.Logger.With("event_id", e.ID, "source", e.Source)
	delay := q.cfg.BaseDelay
	for attempt := 1; attempt <= q.cfg.MaxAttempts; attempt++ {
		retry, err := q.send(e)
		if err == nil {
			log.Info("delivered", "attempt", attempt)
			return
		}
		if !retry {
			log.Error("delivery rejected, not retrying", "attempt", attempt, "error", err)
			return
		}
		if attempt == q.cfg.MaxAttempts {
			log.Error("giving up", "attempts", attempt, "error", err)
			return
		}
		log.Warn("delivery failed, will retry", "attempt", attempt, "wait", delay, "error", err)
		select {
		case <-time.After(delay):
		case <-q.ctx.Done():
			log.Error("shutdown during retry wait, event dropped", "attempt", attempt)
			return
		}
		delay *= 2
		if delay > q.cfg.MaxDelay {
			delay = q.cfg.MaxDelay
		}
	}
}

// send makes one attempt. retry reports whether a later attempt may succeed.
func (q *Queue) send(e Event) (retry bool, err error) {
	req, err := http.NewRequestWithContext(q.ctx, http.MethodPost, q.cfg.TargetURL, bytes.NewReader(e.Body))
	if err != nil {
		return false, err
	}
	if e.ContentType != "" {
		req.Header.Set("Content-Type", e.ContentType)
	}
	req.Header.Set("X-Relay-Source", e.Source)
	req.Header.Set("X-Relay-Event-Id", e.ID)
	resp, err := q.cfg.Client.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return false, nil
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout:
		return true, fmt.Errorf("target answered %d", resp.StatusCode)
	default:
		return false, fmt.Errorf("target answered %d", resp.StatusCode)
	}
}
