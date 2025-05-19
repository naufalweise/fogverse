import subprocess
import yaml

def get_config():
    try:
        with open("benchmark-cluster-config.yaml", "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print("YAML file not found.")
    except yaml.YAMLError as e:
        print("PARSING ERROR:", e)

def run_pb_script(params, algorithm):
    script_args = ['python', './experiments/utils/bromin_bromax/get_pb.py']
    script_args.extend(['--algorithm', algorithm])

    for param, value in params.items():
        if param == 'algorithm':
            continue  # already handled.
        script_args.append(f'--{param}')
        script_args.append(str(value))

    result = subprocess.run(script_args, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"[{algorithm}] ERROR: {result.stderr.strip()}")
        return None, None

    try:
        num_partitions, num_brokers = result.stdout.strip().split()
        return num_partitions, num_brokers
    except ValueError:
        print(f"[{algorithm}] Unexpected output: {result.stdout.strip()}")
        return None, None

def main():
    config = get_config()
    if not config:
        return

    params = config['partitioning_params']
    algorithms = params.get('algorithm', [])

    if not isinstance(algorithms, list):
        algorithms = [algorithms]

    for algo in algorithms:
        partitions, brokers = run_pb_script(params, algo)
        if partitions and brokers:
            print(f"[{algo}] Partitions: {partitions}, Brokers: {brokers}")

if __name__ == "__main__":
    main()
