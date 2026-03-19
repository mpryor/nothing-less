#!/usr/bin/env python3
"""Simulate `kubectl get events -A -w` output for a busy cluster."""

import random
import sys
import time

NAMESPACES = [
    "default",
    "kube-system",
    "monitoring",
    "ingress-nginx",
    "cert-manager",
    "argocd",
    "logging",
    "payments",
    "auth",
    "api-gateway",
    "data-pipeline",
    "ml-serving",
]

PODS = {
    "default": ["web-frontend-{r}", "api-server-{r}", "worker-{r}"],
    "kube-system": [
        "coredns-{r}",
        "kube-proxy-{r}",
        "etcd-master-{r}",
        "kube-apiserver-{r}",
    ],
    "monitoring": [
        "prometheus-server-{r}",
        "grafana-{r}",
        "alertmanager-{r}",
        "node-exporter-{r}",
    ],
    "ingress-nginx": ["nginx-controller-{r}", "default-backend-{r}"],
    "cert-manager": ["cert-manager-{r}", "cert-manager-webhook-{r}"],
    "argocd": [
        "argocd-server-{r}",
        "argocd-repo-server-{r}",
        "argocd-application-controller-{r}",
    ],
    "logging": ["fluentd-{r}", "elasticsearch-{r}", "kibana-{r}"],
    "payments": ["payment-processor-{r}", "stripe-webhook-{r}", "invoice-worker-{r}"],
    "auth": ["auth-service-{r}", "oauth-proxy-{r}", "session-store-{r}"],
    "api-gateway": ["kong-proxy-{r}", "rate-limiter-{r}"],
    "data-pipeline": [
        "spark-driver-{r}",
        "kafka-consumer-{r}",
        "flink-taskmanager-{r}",
    ],
    "ml-serving": ["model-server-{r}", "feature-store-{r}", "prediction-api-{r}"],
}

NODES = [f"node-{i}" for i in range(1, 13)]

# (type, reason, kind, message_template, weight)
EVENTS = [
    ("Normal", "Scheduled", "Pod", "Successfully assigned {ns}/{pod} to {node}", 15),
    ("Normal", "Pulling", "Pod", 'Pulling image "{image}"', 12),
    (
        "Normal",
        "Pulled",
        "Pod",
        'Successfully pulled image "{image}" in {pull_time}',
        12,
    ),
    ("Normal", "Created", "Pod", "Created container {container}", 12),
    ("Normal", "Started", "Pod", "Started container {container}", 12),
    ("Normal", "Killing", "Pod", "Stopping container {container}", 5),
    (
        "Warning",
        "BackOff",
        "Pod",
        "Back-off restarting failed container {container} in pod {pod}",
        8,
    ),
    ("Warning", "Failed", "Pod", "Error: ImagePullBackOff", 4),
    ("Warning", "Failed", "Pod", "Error: CrashLoopBackOff", 6),
    (
        "Warning",
        "Unhealthy",
        "Pod",
        "Readiness probe failed: HTTP probe failed with statuscode: 503",
        5,
    ),
    ("Warning", "Unhealthy", "Pod", "Liveness probe failed: connection refused", 4),
    (
        "Warning",
        "FailedMount",
        "Pod",
        'MountVolume.SetUp failed for volume "config" : configmap "app-config" not found',
        2,
    ),
    (
        "Warning",
        "OOMKilled",
        "Pod",
        "Container {container} exceeded memory limit (512Mi)",
        3,
    ),
    (
        "Normal",
        "ScalingReplicaSet",
        "Deployment",
        "Scaled up replica set {pod} to {replicas}",
        6,
    ),
    (
        "Normal",
        "ScalingReplicaSet",
        "Deployment",
        "Scaled down replica set {pod} to {replicas}",
        4,
    ),
    ("Normal", "SuccessfulCreate", "ReplicaSet", "Created pod: {pod}", 5),
    ("Normal", "SuccessfulDelete", "ReplicaSet", "Deleted pod: {pod}", 3),
    (
        "Warning",
        "FailedCreate",
        "ReplicaSet",
        'Error creating: pods "{pod}" is forbidden: exceeded quota',
        2,
    ),
    ("Normal", "LeaderElection", "Endpoints", "master-{r} became leader", 2),
    ("Normal", "NodeReady", "Node", "Node {node} status is now: NodeReady", 3),
    ("Warning", "NodeNotReady", "Node", "Node {node} status is now: NodeNotReady", 2),
    (
        "Warning",
        "EvictionThresholdMet",
        "Node",
        "Attempting to reclaim ephemeral-storage on {node}",
        2,
    ),
    ("Normal", "Sync", "Ingress", "Scheduled for sync", 3),
    ("Normal", "CertIssued", "Certificate", "Certificate issued successfully", 2),
    (
        "Warning",
        "CertFailed",
        "Certificate",
        "Failed to issue certificate: rate limit exceeded",
        1,
    ),
    ("Normal", "EnsuringLoadBalancer", "Service", "Ensuring load balancer", 2),
    ("Normal", "EnsuredLoadBalancer", "Service", "Ensured load balancer", 2),
]

IMAGES = [
    "nginx:1.25.4",
    "redis:7.2-alpine",
    "postgres:16.2",
    "python:3.13-slim",
    "node:22-alpine",
    "golang:1.23",
    "grafana/grafana:10.4.1",
    "prom/prometheus:v2.51.0",
    "quay.io/argoproj/argocd:v2.10.4",
    "bitnami/kafka:3.7",
    "elasticsearch:8.13.0",
    "fluent/fluentd:v1.16",
    "kong:3.6",
    "ghcr.io/cert-manager/cert-manager-controller:v1.14.4",
    "gcr.io/ml-platform/model-server:v2.8.1",
]

CONTAINERS = [
    "app",
    "sidecar",
    "init-config",
    "istio-proxy",
    "nginx",
    "redis",
    "worker",
]

PULL_TIMES = ["1.2s", "3.8s", "0.9s", "5.1s", "2.3s", "12.4s", "0.4s", "8.7s"]

# Precompute weighted list
_event_pool = []
for e in EVENTS:
    _event_pool.extend([e] * e[4])


def _rand_hex(n: int = 5) -> str:
    return "".join(random.choices("abcdef0123456789", k=n))


def _rand_suffix() -> str:
    return f"{_rand_hex(5)}-{_rand_hex(5)}"


def gen_event() -> str:
    ns = random.choice(NAMESPACES)
    pod_templates = PODS[ns]
    pod = random.choice(pod_templates).format(r=_rand_suffix())
    node = random.choice(NODES)
    image = random.choice(IMAGES)
    container = random.choice(CONTAINERS)
    replicas = random.randint(1, 5)
    pull_time = random.choice(PULL_TIMES)

    ev_type, reason, kind, msg_tmpl, _ = random.choice(_event_pool)
    msg = msg_tmpl.format(
        ns=ns,
        pod=pod,
        node=node,
        image=image,
        container=container,
        replicas=replicas,
        pull_time=pull_time,
        r=_rand_hex(8),
    )

    last_seen = f"{random.randint(0, 59)}s"

    # Match kubectl get events -A -w header:
    # NAMESPACE   LAST SEEN   TYPE      REASON    OBJECT       MESSAGE
    obj = f"{kind.lower()}/{pod}"
    return f"{ns:<18s} {last_seen:<12s} {ev_type:<10s} {reason:<24s} {obj:<52s} {msg}"


def main():
    random.seed(42)  # deterministic output for reproducible demos
    header = f"{'NAMESPACE':<18s} {'LAST SEEN':<12s} {'TYPE':<10s} {'REASON':<24s} {'OBJECT':<52s} MESSAGE"
    print(header, flush=True)

    # Inject known CrashLoopBackOff events for demo reproducibility
    crash_pods = [
        ("payments", "payment-processor-7a3f1-b92e4"),
        ("payments", "stripe-webhook-c81d2-4e0a7"),
        ("auth", "auth-service-e5f93-1d8b6"),
        ("data-pipeline", "kafka-consumer-a42b8-9cf31"),
        ("payments", "invoice-worker-3b7e0-d15a9"),
    ]
    for ns, pod in crash_pods:
        last_seen = f"{random.randint(0, 59)}s"
        msg = f"Back-off restarting failed container app in pod {pod}"
        obj = f"pod/{pod}"
        line = f"{ns:<18s} {last_seen:<12s} {'Warning':<10s} {'CrashLoopBackOff':<24s} {obj:<52s} {msg}"
        print(line, flush=True)
        time.sleep(0.03)

    # Burst: print 40 events quickly to fill the screen
    for _ in range(40):
        print(gen_event(), flush=True)
        time.sleep(0.03)

    # Stream: emit events at a steady clip so counts visibly update
    while True:
        # Small bursts of 2-5 events with short gaps
        burst = random.randint(2, 5)
        for _ in range(burst):
            print(gen_event(), flush=True)
            time.sleep(0.04)
        time.sleep(random.uniform(0.1, 0.4))


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        sys.exit(0)
