kubectl create namespace monitoring
kubectl -n monitoring create -f resources/kubernetes-deployments/prometheus-operator-deployment.yaml
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/prometheus-additional.yaml
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/strimzi-pod-monitor.yaml
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/prometheus-rules.yaml
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/prometheus.yaml

