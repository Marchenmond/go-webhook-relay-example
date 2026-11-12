package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testSecret makes a fresh random secret for each test; none is stored in code.
func testSecret(t *testing.T) []byte {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return []byte(hex.EncodeToString(b))
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newRelay(t *testing.T, target string, secret []byte, maxBody int64) (*httptest.Server, *Queue) {
	t.Helper()
	q := NewQueue(QueueConfig{
		TargetURL:   target,
		Workers:     1,
		MaxAttempts: 4,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    20 * time.Millisecond,
		Logger:      quiet(),
	})
	h := &Handler{Secret: secret, MaxBodyBytes: maxBody, Queue: q, Logger: quiet()}
	srv := httptest.NewServer(h.Routes())
	t.Cleanup(func() {
		srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = q.Close(ctx)
	})
	return srv, q
}

func post(t *testing.T, url string, body []byte, sig string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if sig != "" {
		req.Header.Set(SignatureHeader, sig)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestVerify(t *testing.T) {
	secret := testSecret(t)
	body := []byte(`{"a":1}`)
	good := Sign(secret, body)
	cases := map[string]bool{
		good:                                true,
		"":                                  false,
		"sha256=":                           false,
		"sha256=zz":                         false,
		strings.TrimPrefix(good, "sha256="): false, // missing prefix
		Sign([]byte("other"), body):         false,
	}
	for hdr, want := range cases {
		if got := Verify(secret, body, hdr); got != want {
			t.Errorf("Verify(%q) = %v, want %v", hdr, got, want)
		}
	}
	if Verify(secret, []byte(`{"a":2}`), good) {
		t.Error("signature must not match a different body")
	}
}

func TestValidSignatureIsForwarded(t *testing.T) {
	var got atomic.Value
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.Store(r.Header.Get("X-Relay-Source") + "|" + string(b))
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	secret := testSecret(t)
	srv, _ := newRelay(t, target.URL, secret, 1024)
	body := []byte(`{"order":42}`)

	resp := post(t, srv.URL+"/hooks/shop", body, Sign(secret, body))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	waitFor(t, func() bool { return got.Load() != nil })
	if want := `shop|{"order":42}`; got.Load() != want {
		t.Errorf("target received %v, want %s", got.Load(), want)
	}
}

func TestInvalidSignatureRejected(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer target.Close()

	secret := testSecret(t)
	srv, q := newRelay(t, target.URL, secret, 1024)
	body := []byte(`{"order":42}`)

	for name, sig := range map[string]string{
		"missing": "",
		"wrong":   Sign(testSecret(t), body),
		"garbage": "sha256=not-hex",
	} {
		resp := post(t, srv.URL+"/hooks/shop", body, sig)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", name, resp.StatusCode)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if hits.Load() != 0 || q.Len() != 0 {
		t.Errorf("rejected requests must not reach the queue (hits=%d, queued=%d)", hits.Load(), q.Len())
	}
}

func TestBodyTooLarge(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()

	secret := testSecret(t)
	srv, _ := newRelay(t, target.URL, secret, 16)
	body := bytes.Repeat([]byte("x"), 100)

	resp := post(t, srv.URL+"/hooks/shop", body, Sign(secret, body))
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

func TestRetryUntilTargetAccepts(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 { // fail twice, then accept
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	secret := testSecret(t)
	srv, q := newRelay(t, target.URL, secret, 1024)
	body := []byte(`{"retry":true}`)

	resp := post(t, srv.URL+"/hooks/shop", body, Sign(secret, body))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	waitFor(t, func() bool { return calls.Load() >= 3 })
	time.Sleep(50 * time.Millisecond)
	if n := calls.Load(); n != 3 {
		t.Errorf("target called %d times, want exactly 3 (2 failures + 1 success)", n)
	}
	if q.Len() != 0 {
		t.Errorf("queue should be empty, has %d", q.Len())
	}
}

func TestClientErrorIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer target.Close()

	secret := testSecret(t)
	srv, _ := newRelay(t, target.URL, secret, 1024)
	body := []byte(`{}`)
	post(t, srv.URL+"/hooks/shop", body, Sign(secret, body))
	waitFor(t, func() bool { return calls.Load() >= 1 })
	time.Sleep(80 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Errorf("a 400 must not be retried, got %d calls", n)
	}
}

func TestHealthz(t *testing.T) {
	srv, _ := newRelay(t, "http://127.0.0.1:1", testSecret(t), 1024)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
