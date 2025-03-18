import subprocess
result = subprocess.run("minikube ip", stdout=subprocess.PIPE, text=True)
ip = result.stdout.strip()

result = subprocess.run('kubectl get svc my-cluster-kafka-external-bootstrap -n kafka -o jsonpath="{.spec.ports[0].nodePort}"', stdout=subprocess.PIPE, text=True)
port = result.stdout.strip()

print(f"{ip}:{port}")
