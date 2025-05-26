import subprocess
import time
import sys

# Configuration
NAMESPACE = "kafka"  # Match your Kafka cluster's namespace
DEPLOYMENT_NAME = "kafka-producer-scaler" # Name of the producer deployment
INCREMENT_INTERVAL_SECONDS = 60 # Increase by one replica every 60 seconds (1 minute)
MAX_REPLICAS = 2 # Maximum number of replicas to scale up to

def get_current_replicas(namespace, deployment_name):
    """Fetches the current number of replicas for a given deployment."""
    try:
        cmd = ["kubectl", "get", "deployment", deployment_name, "-n", namespace, "-o=jsonpath='{.spec.replicas}'"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return int(result.stdout.strip().strip("'"))
    except subprocess.CalledProcessError as e:
        print(f"Error getting current replicas: {e}", file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)
        return -1
    except ValueError:
        print(f"Could not parse replica count from: {result.stdout}", file=sys.stderr)
        return -1

def scale_deployment(namespace, deployment_name, new_replicas):
    """Scales a deployment to the specified number of replicas."""
    try:
        cmd = ["kubectl", "scale", "--replicas", str(new_replicas), "deployment", deployment_name, "-n", namespace]
        subprocess.run(cmd, check=True)
        print(f"Scaled deployment '{deployment_name}' to {new_replicas} replicas in namespace '{namespace}'.")
    except subprocess.CalledProcessError as e:
        print(f"Error scaling deployment: {e}", file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)

def main():
    print(f"Starting Kafka producer scaling script for deployment '{DEPLOYMENT_NAME}' in namespace '{NAMESPACE}'.")
    print(f"Will increase replicas by 1 every {INCREMENT_INTERVAL_SECONDS} seconds, up to {MAX_REPLICAS} replicas.")

    current_replicas = get_current_replicas(NAMESPACE, DEPLOYMENT_NAME)
    if current_replicas == -1:
        print("Failed to get initial replica count. Exiting.", file=sys.stderr)
        sys.exit(1)

    if current_replicas >= MAX_REPLICAS:
        print(f"Deployment already at or above max replicas ({MAX_REPLICAS}). Exiting.", file=sys.stderr)
        sys.exit(0)

    print(f"Initial replicas: {current_replicas}")

    while current_replicas < MAX_REPLICas:
        time.sleep(INCREMENT_INTERVAL_SECONDS)
        current_replicas += 1
        if current_replicas > MAX_REPLICAS:
            current_replicas = MAX_REPLICAS # Cap at MAX_REPLICAS

        scale_deployment(NAMESPACE, DEPLOYMENT_NAME, current_replicas)

        if current_replicas == MAX_REPLICAS:
            print(f"Reached maximum replicas of {MAX_REPLICAS}. Script will now exit.")
            break

if __name__ == "__main__":
    main()
