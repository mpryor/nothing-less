#!/usr/bin/env bash
# Simulates a JSON API response (e.g. curl | jq '.[]')
cat <<'EOF'
{"id": 1, "status": "completed", "method": "GET", "path": "/api/users", "latency_ms": 12, "response_code": 200, "user_agent": "Mozilla/5.0"}
{"id": 2, "status": "completed", "method": "POST", "path": "/api/orders", "latency_ms": 89, "response_code": 201, "user_agent": "curl/8.1"}
{"id": 3, "status": "error", "method": "GET", "path": "/api/payments", "latency_ms": 5023, "response_code": 500, "user_agent": "Mozilla/5.0"}
{"id": 4, "status": "completed", "method": "GET", "path": "/api/users/42", "latency_ms": 8, "response_code": 200, "user_agent": "Python/3.13"}
{"id": 5, "status": "completed", "method": "DELETE", "path": "/api/sessions/99", "latency_ms": 15, "response_code": 204, "user_agent": "curl/8.1"}
{"id": 6, "status": "error", "method": "POST", "path": "/api/payments", "latency_ms": 30000, "response_code": 504, "user_agent": "Mozilla/5.0"}
{"id": 7, "status": "completed", "method": "GET", "path": "/api/inventory", "latency_ms": 34, "response_code": 200, "user_agent": "Python/3.13"}
{"id": 8, "status": "completed", "method": "PUT", "path": "/api/users/42", "latency_ms": 22, "response_code": 200, "user_agent": "curl/8.1"}
{"id": 9, "status": "error", "method": "GET", "path": "/api/orders/101", "latency_ms": 4500, "response_code": 503, "user_agent": "Mozilla/5.0"}
{"id": 10, "status": "completed", "method": "GET", "path": "/api/health", "latency_ms": 2, "response_code": 200, "user_agent": "kube-probe/1.28"}
{"id": 11, "status": "completed", "method": "POST", "path": "/api/orders", "latency_ms": 67, "response_code": 201, "user_agent": "Python/3.13"}
{"id": 12, "status": "error", "method": "POST", "path": "/api/payments", "latency_ms": 30000, "response_code": 504, "user_agent": "curl/8.1"}
{"id": 13, "status": "completed", "method": "GET", "path": "/api/users", "latency_ms": 11, "response_code": 200, "user_agent": "Mozilla/5.0"}
{"id": 14, "status": "completed", "method": "GET", "path": "/api/inventory/55", "latency_ms": 18, "response_code": 200, "user_agent": "Python/3.13"}
{"id": 15, "status": "error", "method": "DELETE", "path": "/api/users/7", "latency_ms": 8900, "response_code": 500, "user_agent": "curl/8.1"}
EOF
