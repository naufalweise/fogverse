import random
import time
from fogverse.auto_scaling.policies.spike_detection import SpikeDetectionPolicy

def generate_workload(report_metrics, duration=60, interval=5):
    """
    Generates a random workload in messages_per_second and reports it via the provided callback.
    """
    start_time = time.time()
    while (time.time() - start_time) < duration:
        messages_per_second = random.randint(10, 300)
        metrics = {"messages_per_second": messages_per_second}
        report_metrics(metrics)
        time.sleep(interval)

def main():
    # Starting with 2 replicas.
    current_replicas = [2]  # Using list to allow modification in nested function.

    # Instantiate the SpikeDetectionPolicy.
    policy = SpikeDetectionPolicy(spike_threshold=1.5, max_replicas=10)
    
    def printer(metrics):
        print(f"Generated workload: {metrics['messages_per_second']} messages/s")
        if policy.should_scale(metrics):
            # Calculate new target replicas based on the current number.
            target = policy.target_replicas(current_replicas[0], metrics)
            if target > current_replicas[0]:
                print(f"Scaling triggered! Replicas changing from {current_replicas[0]} to {target}")
                current_replicas[0] = target
            else:
                print("Spike detected, but replica count remains unchanged.")
        else:
            print("No spike detected.")

    generate_workload(printer)

if __name__ == "__main__":
    main()
