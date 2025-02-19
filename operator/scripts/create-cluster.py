import subprocess

result = subprocess.run(["ls", "-l"], capture_output=True, text=True)
print(result.stdout)

subprocess.run("echo Hello, World!", shell=True)

subprocess.run("kubectl get pods", shell=True)