import subprocess
print("ip")
subprocess.run("minikube ip")
print("port")
subprocess.run('kubectl get svc my-cluster-kafka-external-bootstrap -n kafka -o jsonpath="{.spec.ports[0].nodePort}"')