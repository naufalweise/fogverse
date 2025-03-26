# Cluster Operator
Cluster operator manages kafka cluster using kubernetes. It can create kafka cluster, including its brookers, and topic partitions according to bromin and bromax partitioning algorithms.

# Install

- Install kubectl
- Install minikube if you want to run the cluster on your machine
- Start the kubernetes cluster
```
minikube start --memory=4096 # 2GB default memory isn't always enough
```
If using windows, run cmd as admin, then run this command
```
minikube start --memory=4096 --driver=hyperv
```
- Enable metric server
```
minikube addons enable metrics-server
```
- Install strimzi (for managing kafka in kubernetes)
```
kubectl create namespace kafka
kubectl create -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
```
Notes, use namespace kafka in kubernetes.
- Install python dependencies, use virtual env
```
cd operator
python -m venv .env-operator
source .env-operator/bin/activate
pip install -r requirements.txt
```

For windows replace source cmd with
```
.env-operator\Scripts\activate
```

# Running

- Make cluster config. See in examples folder.


- Generate Deployment file
```
python scripts/create-cluster.py --config examples/cluster-config.yaml
```

This will create kubernetes deployment files for the brokers and topics configuration according to bromin/bromax algorithms.

- Deploy Cluster

```
kubectl apply -f resources/kubernetes-deployments/metrics/kafka-monitoring-config.yaml -n kafka
kubectl apply -f out/deployments.yaml -n kafka
```

- Deploy monitoring resources
```
kubectl create namespace monitoring
kubectl -n monitoring create -f resources/kubernetes-deployments/prometheus-operator-deployment.yaml

kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/prometheus-additional.yaml
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/strimzi-pod-monitor.yaml
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/prometheus-rules.yaml
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/prometheus/prometheus.yaml

```
- Install grafana if needed
```
kubectl -n monitoring apply -f resources/kubernetes-deployments/metrics/grafana-install/grafana.yaml

```
- Open grafana/prometheus when ready
```
minikube service prometheus-operated -n monitoring
minikube service grafana -n monitoring
```
For grafana: Default username password is admin / admin.
Add prometheus as datasource with url: http://prometheus-operated:9090.
Import grafana dashboard.

# Basic Usage

## Run kafka producer

```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic
```

## Run kafka consumer

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning
```


# Useful commands

Open minikube dashboard
```
minikube dashboard
```

Stop minikube

```
minikube stop
```

Open operator log
```
kubectl logs deployment/strimzi-cluster-operator -n kafka -f
```

List Pods
```
kubectl get pods -n kafka
```

List Strimzi Objects
```
kubectl get strimzi -n kafka
```


Delete cluster
```
kubectl -n kafka delete $(kubectl get strimzi -o name -n kafka)
kubectl delete pvc -l strimzi.io/name=my-cluster-kafka -n kafka
```

# Monitor CPU Usage
```
kubectl top po -n kafka

```
This tells the cpu usage of pods. 1000m cpu = 1vcpu.
To get the cpu usage of brokers in percent, divide the cpu usage with the cpu limit. you can find the cpu limit in the deployment files or with this command:
```
kubectl get pod <pod-name> -n kafka -o jsonpath="{.spec.containers[*].resources.limits.cpu}"
```