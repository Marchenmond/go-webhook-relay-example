package relay

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// Handler holds what the HTTP routes need.
type Handler struct {
	Secret       []byte
	MaxBodyBytes int64
	Queue        *Queue
	Logger       *slog.Logger
}

// Routes returns the HTTP mux (Go 1.22 method + wildcard patterns).
func (h *Handler) Routes() http.Handler {
	if h.Logger == nil {
		h.Logger = slog.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hooks/{source}", h.receive)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
	return mux
}

func (h *Handler) receive(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	r.Body = http.MaxBytesReader(w, r.Body, h.MaxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			h.Logger.Warn("body too large", "source", source, "limit", tooBig.Limit)
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}
	if !Verify(h.Secret, body, r.Header.Get(SignatureHeader)) {
		h.Logger.Warn("invalid signature", "source", source)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	id := newID()
	err = h.Queue.Enqueue(Event{
		ID:          id,
		Source:      source,
		ContentType: r.Header.Get("Content-Type"),
		Body:        body,
		ReceivedAt:  time.Now(),
	})
	if err != nil {
		h.Logger.Error("could not enqueue", "source", source, "error", err)
		http.Error(w, "queue unavailable, try again later", http.StatusServiceUnavailable)
		return
	}
	h.Logger.Info("accepted", "event_id", id, "source", source, "bytes", len(body))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, `{"id":"`+id+`"}`)
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
