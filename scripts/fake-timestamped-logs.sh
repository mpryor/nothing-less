#!/usr/bin/env bash
# Generate fake log lines with timestamps in local time (America/Denver).
# Dumps ~2 hours of backlog then streams new lines every 1–5 seconds.
# Usage: ./scripts/fake-timestamped-logs.sh | poetry run nless

set -euo pipefail

now=$(date +%s)

emit() {
    local ts_epoch=$1 level=$2 service=$3 msg=$4
    local ts
    ts=$(date -d "@$ts_epoch" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r "$ts_epoch" '+%Y-%m-%d %H:%M:%S')
    echo "$ts,$level,$service,$msg"
}

echo "timestamp,level,service,message"

# --- Backlog: ~2 hours of history ---

# Older events (90–120 min ago)
emit $((now - 7200)) INFO   auth      "Server started on port 8080"
emit $((now - 7140)) INFO   auth      "Connected to database pool (max=20)"
emit $((now - 6900)) INFO   gateway   "Route table loaded: 14 routes"
emit $((now - 6600)) DEBUG  auth      "Health check passed"
emit $((now - 6000)) INFO   billing   "Stripe webhook listener registered"
emit $((now - 5400)) WARN   gateway   "Slow upstream: billing responded in 1.2s"
emit $((now - 4800)) INFO   auth      "Token cache warmed: 342 entries"
emit $((now - 4200)) DEBUG  billing   "Invoice batch #1081 queued"
emit $((now - 3600)) INFO   gateway   "TLS cert renewal scheduled for 2026-04-01"

# Mid-range events (30–60 min ago)
emit $((now - 3000)) WARN   auth      "Rate limit approaching for IP 10.0.3.41 (48/50 req/min)"
emit $((now - 2700)) INFO   billing   "Payment processed: order_8832 \$149.99"
emit $((now - 2400)) ERROR  gateway   "Connection reset by peer: upstream billing:8443"
emit $((now - 2100)) WARN   billing   "Retry 1/3 for charge ch_3Nxk on order_8833"
emit $((now - 1800)) INFO   auth      "User session refreshed: uid=4471"
emit $((now - 1500)) DEBUG  gateway   "Circuit breaker half-open for billing (5 failures in 60s)"
emit $((now - 1200)) INFO   billing   "Retry succeeded for charge ch_3Nxk"
emit $((now - 900))  WARN   auth      "Deprecated OAuth1 token used by client app_legacy_crm"

# Recent burst (last 10 min)
emit $((now - 600))  ERROR  auth      "Failed login: user=admin@corp.io ip=203.0.113.42 (bad password)"
emit $((now - 540))  ERROR  auth      "Failed login: user=admin@corp.io ip=203.0.113.42 (bad password)"
emit $((now - 480))  ERROR  auth      "Failed login: user=admin@corp.io ip=203.0.113.42 (bad password)"
emit $((now - 420))  WARN   auth      "Account locked: admin@corp.io (3 failed attempts)"
emit $((now - 360))  INFO   gateway   "Incoming spike: 1.2k req/s (normal: 400 req/s)"
emit $((now - 300))  WARN   gateway   "Request queue depth: 847 (threshold: 500)"
emit $((now - 240))  ERROR  billing   "Timeout connecting to Stripe API (30s deadline exceeded)"
emit $((now - 180))  ERROR  billing   "Payment failed: order_8841 — upstream timeout"
emit $((now - 120))  WARN   gateway   "5xx rate: 12% (threshold: 5%)"
emit $((now - 60))   ERROR  gateway   "Circuit breaker OPEN for billing (10 failures in 120s)"
emit $((now - 30))   INFO   auth      "Auto-unlock scheduled for admin@corp.io in 15 min"
emit "$now"          WARN   gateway   "Fallback response served for /api/billing/status"

# --- Streaming: new lines every 1–5 seconds ---

services=(auth gateway billing)
seq=0

info_msgs=(
    "Health check passed"
    "Request completed in 42ms"
    "Cache hit ratio: 94%"
    "Connection pool: 12/20 active"
    "Scheduled job completed: cleanup_stale_sessions"
    "User login: uid=7712 ip=10.0.1.55"
    "Webhook delivered to partner_acme (200 OK)"
    "Config reloaded from /etc/app/config.yaml"
    "Background worker picked up job batch_export_1094"
    "TLS handshake completed with upstream in 8ms"
)

warn_msgs=(
    "Slow query: SELECT * FROM orders WHERE... (1.8s)"
    "Memory usage: 78% (threshold: 80%)"
    "Retry 2/3 for webhook delivery to partner_globex"
    "Request queue depth: 612 (threshold: 500)"
    "Deprecated API version v1 called by client mobile_ios"
    "Connection pool near capacity: 18/20"
    "Disk usage on /var/log: 88%"
    "Rate limit warning: IP 10.0.3.41 at 45/50 req/min"
    "Upstream latency spike: billing p99=2.1s"
    "GC pause: 120ms (threshold: 100ms)"
)

error_msgs=(
    "Connection refused: upstream payments:9090"
    "Unhandled exception in /api/orders: NullPointerException"
    "Database query timeout after 30s"
    "TLS certificate verification failed for partner_globex"
    "Out of memory: killed worker pid=4421"
    "Failed to write audit log: disk full"
    "Circuit breaker OPEN for payments (15 failures in 60s)"
    "Request dropped: 503 Service Unavailable"
    "Deadlock detected on table: inventory_locks"
    "Kafka consumer lag: 12,000 messages behind"
)

pick() {
    local -n arr=$1
    echo "${arr[$((RANDOM % ${#arr[@]}))]}"
}

while true; do
    sleep $(( (RANDOM % 5) + 1 ))
    seq=$((seq + 1))
    svc=${services[$((RANDOM % ${#services[@]}))]}

    # Weighted level selection: 50% INFO, 30% WARN, 20% ERROR
    roll=$((RANDOM % 10))
    if (( roll < 5 )); then
        level=INFO
        msg=$(pick info_msgs)
    elif (( roll < 8 )); then
        level=WARN
        msg=$(pick warn_msgs)
    else
        level=ERROR
        msg=$(pick error_msgs)
    fi

    emit "$(date +%s)" "$level" "$svc" "$msg"
done
