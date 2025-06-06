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
kubectl apply -f out/deployments.yaml -n kafka
```

- Wait for the cluster to be ready
```
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka 
```
The above command might timeout if you’re downloading images over a slow connection. If that happens you can always run it again.




# Basic Usage

## Get External Bootstrap Server Address
Run as admin if using hyperv.

```
python scripts/get-bootstrap-server.py
```
You can use this address to access kafka from outside the cluster.

## Run Fogverse
See readme in examples/fogverse folder.

## Run kafka producer

```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic-1
```

## Run kafka consumer

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic-1 --from-beginning
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


Fix DNS Problem
```
kubectl set env deployment strimzi-cluster-operator KUBERNETES_SERVICE_DNS_DOMAIN=cluster.local -n kafka
```
Then wait for rolling update.