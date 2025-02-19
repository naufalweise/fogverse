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

# Running

Deploy Cluster

```
kubectl apply -f operator/resources/kubernetes-deployments -n kafka
```

# Useful commands

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