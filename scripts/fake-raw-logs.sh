#!/usr/bin/env bash
# Simulates raw unstructured log lines (no delimiter)
cat <<'EOF'
[2026-03-18 08:01:12] INFO  server started on port 8080
[2026-03-18 08:01:13] INFO  connected to database postgres://db:5432/app
[2026-03-18 08:01:14] INFO  loading configuration from /etc/app/config.yaml
[2026-03-18 08:01:15] INFO  health check endpoint registered at /health
[2026-03-18 08:01:22] INFO  GET /api/users 200 12ms
[2026-03-18 08:01:23] INFO  POST /api/orders 201 89ms
[2026-03-18 08:01:25] WARN  slow query detected: SELECT * FROM payments WHERE id=42 (3200ms)
[2026-03-18 08:01:26] INFO  GET /api/users/42 200 8ms
[2026-03-18 08:01:28] ERROR connection pool exhausted — retrying in 5s
[2026-03-18 08:01:33] ERROR connection pool exhausted — retrying in 5s
[2026-03-18 08:01:34] INFO  GET /health 200 1ms
[2026-03-18 08:01:35] INFO  DELETE /api/sessions/99 204 15ms
[2026-03-18 08:01:38] ERROR connection pool exhausted — giving up after 3 retries
[2026-03-18 08:01:39] WARN  request timeout: POST /api/payments (30000ms)
[2026-03-18 08:01:40] INFO  PUT /api/users/42 200 22ms
[2026-03-18 08:01:41] INFO  GET /api/inventory 200 34ms
[2026-03-18 08:01:42] ERROR failed to process payment: upstream service unavailable
[2026-03-18 08:01:43] INFO  GET /api/orders/101 200 45ms
[2026-03-18 08:01:44] WARN  memory usage at 89% — consider scaling
[2026-03-18 08:01:45] INFO  POST /api/orders 201 67ms
EOF
