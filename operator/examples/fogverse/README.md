# Fogverse Example

## Running
- cd examples/fogverse
- Create virtual env for fogverse example
- Install requirements
- Get bootstrap server address, see operator readme.
- Run producer
```
set BOOTSTRAP_SERVERS=<bootstrap-server-addr> & python producer.py  
```
- Run consumer in seperate terminal, dont forget to activate the virtual env
```
set CONSUMER_SERVERS=<bootstrap-server-addr> & python consumer.py
```

## Run Fogverse Container in Cluster
- Run this command so when building docker, the image automatically goes to minikube
```
minikube docker-env
```
Copy the generated commands to the terminal.
- Build fogverse image
```
docker build -t fog-prod:1.0.0 -f producer.Dockerfile .
docker build -t fog-cons:1.0.0 -f consumer.Dockerfile .

```
- Deploy fogverse producer, spec is in deployments.yaml
```
kubectl apply -f deployments.yaml -n kafka
```
In this example, producer is deployed as a kubernetes Job because the program stops after finishing its job, consumer is deployed as Pod because the program keeps running. 

To view the jobs and pogs logs, you can use minikube dashboard or this command.
```
kubectl logs job/fogverse-producer -n kafka
kubectl logs fogverse-consumer -n kafka
```

## Notes
The consumer and producer in the examples uses 'my-topic-1' topic. Make sure to specify that topic in cluster-config.yaml before generating and running cluster deployment.