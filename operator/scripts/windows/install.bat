kubectl create namespace kafka
kubectl create -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
kubectl apply -f resources/kubernetes-deployments/metrics/kafka-monitoring-config.yaml -n kafka
