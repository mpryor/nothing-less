#!/bin/bash
# Simulates 'kubectl get pods -A' where some pods have RESTARTS with '(Xs ago)'.
# This reproduces the false column boundary bug in detect_space_splitting_strategy.
#
# Test BEFORE fix:
#   ./scripts/fake-kubectl-pods-mixed.sh | poetry run nless -d 'space+'
#   Expected bugs:
#     - Empty-named column between RESTARTS and AGE
#     - RESTARTS shows '4584 (3m4s' instead of '4584 (3m4s ago)'
#     - Skipped count > 0 in status bar
#
# Test AFTER fix:
#   ./scripts/fake-kubectl-pods-mixed.sh | poetry run nless -d 'space+'
#   Expected:
#     - 6 columns: NAMESPACE, NAME, READY, STATUS, RESTARTS, AGE
#     - RESTARTS shows '4584 (3m4s ago)' intact
#     - Skipped: 0
#     - Pressing ~ says "All logs are being shown"

cat <<'EOF'
NAMESPACE       NAME                                       READY   STATUS             RESTARTS          AGE
default         nginx-deployment-5d7f8c9b47-abc12          1/1     Running            0                 10d
default         nginx-deployment-5d7f8c9b47-def34          1/1     Running            0                 10d
kube-system     coredns-5d78c9869d-xyz34                   1/1     Running            4584 (3m4s ago)   10d
kube-system     etcd-control-plane                         1/1     Running            0                 10d
kube-system     kube-apiserver-control-plane                1/1     Running            3 (2d ago)        10d
monitoring      prometheus-server-7b4f8c9d4f-ghi56         0/1     CrashLoopBackOff   127 (5m12s ago)   5d
monitoring      grafana-6c4d7b9f8-jkl78                    1/1     Running            0                 5d
argocd          argocd-server-abc123                       1/1     Running            1 (4d ago)        30d
EOF

# Keep alive
sleep 300
