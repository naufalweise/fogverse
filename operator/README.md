# Install

- Install kubectl
- Install minikube if you want to run the cluster on your machine
- Start the kubernetes cluster
```
minikube start --memory=4096 # 2GB default memory isn't always enough
```
- Install strimzi (for managing kafka in kubernetes)
```
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
```
Notes, use namespace kafka in kubernetes.
- Install python dependencies, use virtual env
```
cd operator
python -m venv .env-operator
source .env-operator/bin/activate
pip install -r requirements
```

# Running

- Make cluster config. See in examples folder.


- Generate Deployment file
```
python scripts/create-cluster.py --config examples/cluster-config.yaml
```

- Deploy Cluster

```
kubectl apply -f out/deployments.yaml -n kafka
```

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