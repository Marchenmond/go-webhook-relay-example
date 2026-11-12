# Webhook Relay Example (Go)

**Result: signed webhooks are verified, queued and forwarded to your system with automatic retries, so a temporary outage on your side no longer loses events (7 tests passing, race detector clean).**

| | Before | After |
|---|---|---|
| Fake or tampered requests | Accepted by mistake | Rejected with 401 (HMAC-SHA256 signature) |
| Huge payloads | Can exhaust memory | Rejected with 413 at a configurable limit |
| Your backend is down for a minute | Event lost | Retried with growing waits until it answers |
| Restart / deploy | Requests cut in half | Clean shutdown, queue drained first |

## The problem

Services such as payment providers, shops and form tools send webhooks (an HTTP request saying "something happened"). They give up quickly if your server is slow or down, and anyone who finds the URL can send fake events. A small relay in front of your system solves both: it checks who sent the request, answers fast, and takes care of delivery.

## What it does

- `POST /hooks/{source}` accepts a webhook. `{source}` is just a label (for example `shop`) passed on to the destination.
- Verifies an HMAC-SHA256 signature of the raw body, compared in constant time. The secret comes from an environment variable and is never in the code.
- Limits the body size (default 1 MiB).
- Stores the event in an in-memory queue and answers `202 Accepted` right away.
- Workers forward the event to your `TARGET_URL`, retrying on network errors, `5xx`, `429` and `408` with a doubling wait (1s, 2s, 4s ... up to a cap). Other `4xx` answers are not retried.
- `GET /healthz` for load balancers.
- Structured JSON logs (`log/slog`) and graceful shutdown on Ctrl+C / SIGTERM.
- Go standard library only, Go 1.22+.

The queue lives in memory, so events still waiting when the process dies are lost. See "Easy to adapt".

## Usage

```bash
export WEBHOOK_SECRET="$(openssl rand -hex 32)"
export TARGET_URL="http://localhost:9000/events"   # where events are forwarded
go run ./cmd/relay
```

Send a signed request. The signature is `sha256=` plus the hex HMAC-SHA256 of the exact body bytes:

```bash
BODY='{"order_id":1001,"status":"paid"}'
SIG="sha256=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" -hex | sed 's/^.* //')"

curl -i -X POST http://localhost:8080/hooks/shop \
  -H "Content-Type: application/json" \
  -H "X-Signature-256: $SIG" \
  -d "$BODY"
# HTTP/1.1 202 Accepted
# {"id":"3f9c1a7b2d4e5f60"}
```

Without the header, or with a wrong one, the answer is `401 invalid signature` and nothing is queued.

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `WEBHOOK_SECRET` | required | Shared secret used to verify signatures |
| `TARGET_URL` | required | Where accepted events are forwarded (POST) |
| `ADDR` | `:8080` | Listen address |
| `MAX_BODY_BYTES` | `1048576` | Largest accepted body |
| `QUEUE_SIZE` | `100` | Queue capacity; when full the relay answers `503` |
| `WORKERS` | `2` | Parallel deliveries |
| `MAX_ATTEMPTS` | `5` | Tries per event, including the first |
| `BASE_DELAY_MS` | `1000` | Wait before the second try (doubles each time) |
| `MAX_DELAY_MS` | `30000` | Upper limit for the wait |

Optional container: `docker build -t webhook-relay . && docker run -e WEBHOOK_SECRET -e TARGET_URL -p 8080:8080 webhook-relay`.

## Tests

```bash
go test ./... -race        # 7 tests, all passing
```

They use `httptest` servers and a fresh random secret per test. They cover valid and invalid signatures (missing, wrong, malformed), a body over the limit, forwarding of a valid event, a destination that fails twice and then accepts (exactly 3 calls), no retry on a `400`, and `/healthz`. CI runs `gofmt`, `go vet` and the tests on every push.

## Easy to adapt

Typical customizations for a client: a different signature header or scheme (for example a provider's own format), one secret per source, a durable queue (SQLite, Redis or a file) so nothing is lost on restart, a dead-letter list for events that never got through, delivery to several destinations, metrics, or deployment (Docker, systemd, a small VPS).

---
## Need something like this?

Built by Gabriel Montemór · Marchenmond. Available on Upwork for small fixes, integrations and automations: send a message through my Upwork profile.
