import math
import yaml

from collections import OrderedDict

from experiments.throughput import main as throughput_main
from experiments.replication_latency import main as replication_latency_main
from experiments.open_file_handles import main as open_file_handles_main
from experiments.unavailability_time import main as unavailability_time_main

def format_scientific(value):
    iv = int(value)
    if iv == 0:
        return "0"
    # Determine exponent.
    exp = int(math.floor(math.log10(abs(iv))))
    # For small single‐digit values, just return as-is.
    if exp == 0:
        return str(iv)
    # Compute mantissa.
    mantissa = value / (10 ** exp)
    # Count significant digits (strip trailing zeros).
    sig = str(abs(iv)).rstrip('0')
    precision = max(len(sig) - 1, 0)
    if precision == 0:
        mantissa_str = f"{int(mantissa)}."
    else:
        mantissa_str = f"{mantissa:.{precision}f}"
    # Format exponent with sign.
    sign = '+' if exp >= 0 else '-'
    return f"{mantissa_str}e{sign}{abs(exp)}"

def main():
    # Run the experiments and get the results.
    producer_throughput, consumer_throughput = throughput_main()
    replication_latency = replication_latency_main()
    open_file_handles = open_file_handles_main()
    unavailability_time = unavailability_time_main()

    # Load the default cluster configuration.
    with open("default-cluster-config.yaml", "r") as f:
        config = yaml.safe_load(f)

    params = config["partitioning_params"]

    # Update parameters with values from experiments.
    params["Tp"] = producer_throughput
    params["Tc"] = consumer_throughput
    params["lr"] = replication_latency
    params["Hmax"] = open_file_handles
    params["u"] = unavailability_time

    # Update the configuration with the new parameters.
    ordered_config = OrderedDict()
    ordered_config["topics"] = [f"'{t}'" for t in config["topics"]]  # for formatting with quotes.
    ordered_config["partitioning_params"] = OrderedDict([
        ("algorithm", [f"'{a}'" for a in params["algorithm"]]),
        ("T", format_scientific(params["T"])),
        ("L", params["L"]),
        ("U", params["U"]),
        ("Tp", format_scientific(params["Tp"])),
        ("Tc", format_scientific(params["Tc"])),
        ("Hmax", params["Hmax"]),
        ("lr", format_scientific(params["lr"])),
        ("u", format_scientific(params["u"])),
        ("c", params["c"]),
        ("r", params["r"]),
        ("B", params["B"]),
    ])

    # Create the new YAML file with the updated configuration.
    with open("benchmark-cluster-config.yaml", "w") as f:
        # Manually format the YAML to match the desired style.
        f.write(f"topics: [ {', '.join(ordered_config['topics'])} ]\n")
        f.write("partitioning_params:\n")
        for key, value in ordered_config["partitioning_params"].items():
            if isinstance(value, list):
                f.write(f"  {key}: [ {', '.join(value)} ]\n")
            else:
                f.write(f"  {key}: {value}\n")

if __name__ == "__main__":
    main()
