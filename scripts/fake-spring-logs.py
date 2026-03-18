#!/usr/bin/env python3
"""Simulate Spring Boot application logs with a recurring database connection error."""

import random
import sys
import time
from datetime import datetime, timedelta

CLASSES = [
    "c.a.p.PaymentController",
    "c.a.p.OrderService",
    "c.a.p.UserRepository",
    "c.a.p.InventoryService",
    "c.a.p.NotificationWorker",
    "c.a.p.AuthFilter",
    "c.a.p.CacheManager",
    "c.a.p.HealthCheckEndpoint",
    "c.a.p.RateLimiter",
    "c.a.p.MetricsExporter",
    "o.s.w.s.DispatcherServlet",
    "o.a.c.h.Http11Processor",
    "o.h.e.j.i.JdbcEnvironmentInitiator",
    "o.s.b.a.e.w.EndpointLinksResolver",
    "c.a.p.DatabaseConnectionPool",
]

ENDPOINTS = [
    "/api/v1/payments",
    "/api/v1/orders",
    "/api/v1/users",
    "/api/v1/inventory",
    "/api/v1/notifications",
    "/health",
    "/api/v1/auth/token",
    "/api/v1/cart",
    "/metrics",
]

REQUEST_IDS = [
    f"{random.randint(1000, 9999)}-{random.randint(100, 999)}" for _ in range(20)
]

# Normal log templates (class, message)
NORMAL_LOGS = [
    ("c.a.p.PaymentController", "Processing payment request for order #{order_id}"),
    (
        "c.a.p.PaymentController",
        "Payment completed successfully for order #{order_id} — ${amount}",
    ),
    ("c.a.p.OrderService", "Order #{order_id} status updated to CONFIRMED"),
    ("c.a.p.OrderService", "Fetching order details for customer {customer_id}"),
    (
        "c.a.p.UserRepository",
        "User lookup completed in {latency}ms [userId={customer_id}]",
    ),
    ("c.a.p.InventoryService", "Stock check passed for SKU-{sku} (remaining: {stock})"),
    (
        "c.a.p.InventoryService",
        "Reserving {qty} units of SKU-{sku} for order #{order_id}",
    ),
    (
        "c.a.p.NotificationWorker",
        "Email notification queued for customer {customer_id}",
    ),
    ("c.a.p.NotificationWorker", "SMS dispatch completed [recipient=+1-555-{phone}]"),
    ("c.a.p.AuthFilter", "JWT token validated for session {session_id}"),
    ("c.a.p.AuthFilter", "Rate limit check passed for IP {ip}"),
    ("c.a.p.CacheManager", "Cache HIT for key=user:{customer_id} [ttl=240s]"),
    ("c.a.p.CacheManager", "Cache MISS for key=inventory:{sku} — fetching from DB"),
    ("c.a.p.HealthCheckEndpoint", "Health check OK [db=UP, redis=UP, kafka=UP]"),
    ("c.a.p.RateLimiter", "Request allowed for {ip} [{count}/100 in window]"),
    (
        "c.a.p.MetricsExporter",
        "Metrics snapshot exported — {metric_count} metrics, {latency}ms",
    ),
    ("o.s.w.s.DispatcherServlet", "GET {endpoint} 200 {latency}ms"),
    ("o.s.w.s.DispatcherServlet", "POST {endpoint} 201 {latency}ms"),
    ("o.a.c.h.Http11Processor", "Connection accepted from {ip}:{port}"),
]

WARN_LOGS = [
    (
        "c.a.p.DatabaseConnectionPool",
        "Connection pool utilization at {pool_pct}% [{pool_active}/{pool_max} active]",
    ),
    (
        "c.a.p.DatabaseConnectionPool",
        "Slow query detected: {latency}ms for SELECT on payments [txn={order_id}]",
    ),
    ("c.a.p.RateLimiter", "Rate limit threshold approaching for IP {ip} [{count}/100]"),
    ("c.a.p.CacheManager", "Redis latency elevated: {latency}ms [threshold=50ms]"),
    (
        "o.a.c.h.Http11Processor",
        "Request timeout approaching for {endpoint} [{latency}ms elapsed]",
    ),
]

# The error we want the user to chase down
DB_ERROR_CLASS = "c.a.p.DatabaseConnectionPool"
DB_ERROR_MSG = "Failed to acquire connection from pool — timeout after 30000ms"
DB_STACKTRACE = [
    "org.springframework.dao.DataAccessResourceFailureException: Failed to acquire connection from pool",
    "    at org.springframework.jdbc.datasource.DataSourceUtils.getConnection(DataSourceUtils.java:82)",
    "    at org.hibernate.engine.jdbc.connections.internal.DatasourceConnectionProviderImpl.getConnection(DatasourceConnectionProviderImpl.java:122)",
    "    at com.acme.payments.DatabaseConnectionPool.borrowConnection(DatabaseConnectionPool.java:147)",
    "    at com.acme.payments.OrderService.processOrder(OrderService.java:89)",
    "    at com.acme.payments.PaymentController.submitPayment(PaymentController.java:54)",
    "Caused by: java.sql.SQLTransientConnectionException: HikariPool-1 — Connection is not available, request timed out after 30000ms",
    "    at com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:696)",
    "    at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:197)",
    "    at com.zaxxer.hikari.HikariDataSource.getConnection(HikariDataSource.java:100)",
    "    ... 42 more",
]

# Secondary errors caused by the DB issue
SECONDARY_ERRORS = [
    (
        "c.a.p.PaymentController",
        "Payment processing failed for order #{order_id} — upstream timeout",
    ),
    (
        "c.a.p.OrderService",
        "Transaction rolled back for order #{order_id}: connection pool exhausted",
    ),
    (
        "c.a.p.NotificationWorker",
        "Failed to persist notification record: connection unavailable",
    ),
]


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def _vars() -> dict:
    return {
        "order_id": random.randint(100000, 999999),
        "amount": f"{random.uniform(9.99, 499.99):.2f}",
        "customer_id": f"cust-{random.randint(1000, 9999)}",
        "latency": random.randint(2, 800),
        "sku": random.randint(10000, 99999),
        "stock": random.randint(1, 500),
        "qty": random.randint(1, 10),
        "phone": f"{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        "session_id": f"sess-{random.randint(10000, 99999)}",
        "ip": f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "port": random.randint(30000, 65535),
        "endpoint": random.choice(ENDPOINTS),
        "count": random.randint(50, 95),
        "pool_pct": random.randint(80, 98),
        "pool_active": random.randint(18, 24),
        "pool_max": 25,
        "metric_count": random.randint(120, 340),
    }


def gen_normal(dt: datetime) -> list[str]:
    cls, tmpl = random.choice(NORMAL_LOGS)
    msg = tmpl.format(**_vars())
    return [f"{_ts(dt)}  INFO 1 --- [{random.choice(REQUEST_IDS)}] {cls:<45s} : {msg}"]


def gen_warn(dt: datetime) -> list[str]:
    cls, tmpl = random.choice(WARN_LOGS)
    msg = tmpl.format(**_vars())
    return [f"{_ts(dt)}  WARN 1 --- [{random.choice(REQUEST_IDS)}] {cls:<45s} : {msg}"]


def gen_db_error(dt: datetime) -> list[str]:
    lines = [
        f"{_ts(dt)} ERROR 1 --- [{random.choice(REQUEST_IDS)}] {DB_ERROR_CLASS:<45s} : {DB_ERROR_MSG}"
    ]
    for trace_line in DB_STACKTRACE:
        lines.append(trace_line)
    # Follow up with a secondary error
    cls, tmpl = random.choice(SECONDARY_ERRORS)
    msg = tmpl.format(**_vars())
    dt2 = dt + timedelta(milliseconds=random.randint(5, 50))
    lines.append(
        f"{_ts(dt2)} ERROR 1 --- [{random.choice(REQUEST_IDS)}] {cls:<45s} : {msg}"
    )
    return lines


def main():
    dt = datetime.now() - timedelta(seconds=30)

    # Burst: healthy startup logs
    for _ in range(15):
        dt += timedelta(milliseconds=random.randint(10, 200))
        for line in gen_normal(dt):
            print(line, flush=True)
        time.sleep(0.03)

    # First warning signs
    for _ in range(3):
        dt += timedelta(milliseconds=random.randint(100, 500))
        for line in gen_warn(dt):
            print(line, flush=True)
        time.sleep(0.05)

    # More normal, then errors start
    error_count = 0
    while True:
        dt += timedelta(milliseconds=random.randint(50, 800))
        roll = random.random()

        if roll < 0.12:
            # DB connection error
            for line in gen_db_error(dt):
                print(line, flush=True)
            error_count += 1
            time.sleep(random.uniform(0.05, 0.15))
        elif roll < 0.25:
            for line in gen_warn(dt):
                print(line, flush=True)
            time.sleep(random.uniform(0.1, 0.4))
        else:
            for line in gen_normal(dt):
                print(line, flush=True)
            time.sleep(random.uniform(0.1, 0.5))


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        sys.exit(0)
