#!/bin/bash
# Simulates 'kubectl get pods -A' where some pods have RESTARTS with '(Xs ago)'.
#
# Test:
#   ./scripts/fake-kubectl-pods-mixed.sh | poetry run nless
#
# What to check:
#   1. Delimiter should auto-infer as space+ (shown in status bar)
#   2. 6 columns: NAMESPACE, NAME, READY, STATUS, RESTARTS, AGE
#   3. RESTARTS shows '2000 (8s ago)' intact (not split)
#   4. No empty-named column
#   5. Press U on NAMESPACE to create unique key — Skipped should stay 0
#   6. Press ~ — should say "All logs are being shown"

cat <<'EOF'
NAMESPACE       NAME                                       READY   STATUS             RESTARTS          AGE
default         nginx-deployment-5d7f8c9b47-abc12          1/1     Running            0                 10d
default         nginx-deployment-5d7f8c9b47-def34          1/1     Running            0                 10d
kube-system     coredns-5d78c9869d-xyz34                   1/1     Running            2000 (8s ago)     10d
kube-system     etcd-control-plane                         1/1     Running            0                 10d
kube-system     kube-apiserver-control-plane                1/1     Running            3 (2d ago)        10d
monitoring      prometheus-server-7b4f8c9d4f-ghi56         0/1     CrashLoopBackOff   127 (5m12s ago)   5d
monitoring      grafana-6c4d7b9f8-jkl78                    1/1     Running            0                 5d
argocd          argocd-server-abc123                       1/1     Running            1 (4d ago)        30d
EOF

sleep 300
