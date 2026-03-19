#!/bin/bash
# Simulates 'kubectl get pods -A -w' where RESTARTS '(Xs ago)' values arrive
# via streaming after the initial batch is clean.
#
# Test:
#   ./scripts/fake-kubectl-pods.sh | poetry run nless
#
# Watch the streaming lines arrive — RESTARTS should stay intact, no Skipped.

cat <<'EOF'
NAMESPACE       NAME                                       READY   STATUS    RESTARTS          AGE
default         nginx-deployment-5d7f8c9b47-abc12          1/1     Running   0                 10d
default         nginx-deployment-5d7f8c9b47-def34          1/1     Running   0                 10d
kube-system     coredns-5d78c9869d-xyz34                   1/1     Running   0                 10d
kube-system     etcd-control-plane                         1/1     Running   0                 10d
kube-system     kube-apiserver-control-plane                1/1     Running   0                 10d
monitoring      prometheus-server-7b4f8c9d4f-ghi56         1/1     Running   0                 5d
monitoring      grafana-6c4d7b9f8-jkl78                    1/1     Running   0                 5d
EOF

sleep 2
echo "kube-system     coredns-5d78c9869d-xyz34                   0/1     CrashLoopBackOff   4584 (3m4s ago)   10d"
sleep 1
echo "kube-system     coredns-5d78c9869d-xyz34                   0/1     CrashLoopBackOff   4585 (45s ago)    10d"
sleep 1
echo "kube-system     coredns-5d78c9869d-xyz34                   1/1     Running            4585 (10s ago)    10d"
sleep 1
echo "monitoring      prometheus-server-7b4f8c9d4f-ghi56         0/1     Error              1 (5s ago)        5d"
sleep 1
echo "monitoring      prometheus-server-7b4f8c9d4f-ghi56         1/1     Running            1 (2s ago)        5d"

sleep 300
