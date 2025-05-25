import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def process_data(data):
    """
    Flattens the nested JSON data into a pandas DataFrame.
    """
    flat_data = []
    for config_name, config_data in data.items():
        for scenario_name, scenario_data in config_data.items():
            if isinstance(scenario_data, dict) and ("BroMin" in config_data or "BroMax" in config_data):
                # This is to handle the nested structure of the JSON.
                for message_size, test_data in scenario_data.items():
                     if isinstance(test_data, dict) and "producer" in test_data:
                        record = {
                            "config": config_name,
                            "scenario": scenario_name,
                            "message_size": message_size,
                            "partitions": scenario_data.get("partitions"),
                            "brokers": scenario_data.get("brokers"),
                            "producer_throughput_mbps": test_data.get("producer", {}).get("throughput_mbps"),
                            "producer_latency_ms": test_data.get("producer", {}).get("latency_ms"),
                            "producer_status": test_data.get("producer", {}).get("status"),
                            "consumer_throughput_mbps": test_data.get("consumer", {}).get("throughput_mbps"),
                            "consumer_fetch_ms": test_data.get("consumer", {}).get("fetch_ms"),
                            "consumer_status": test_data.get("consumer", {}).get("status"),
                        }
                        flat_data.append(record)

    # Correctly handle cases where "BroMin" or "BroMax" is nested one level deeper.
    processed_data = []
    for config_name, config_results in data.items():
        for scenario_key, scenario_value in config_results.items():
            if scenario_key in ["BroMin", "BroMax"]:
                for message_size, message_data in scenario_value.items():
                    if isinstance(message_data, dict) and "producer" in message_data:
                        row = {
                            "config": config_name,
                            "scenario": scenario_key,
                            "message_size": message_size,
                            "brokers": scenario_value["brokers"],
                            "partitions": scenario_value["partitions"],
                            "producer_throughput_mbps": message_data["producer"]["throughput_mbps"],
                            "producer_latency_ms": message_data["producer"]["latency_ms"],
                            "producer_status": message_data["producer"]["status"],
                            "consumer_throughput_mbps": message_data["consumer"]["throughput_mbps"],
                            "consumer_fetch_ms": message_data["consumer"]["fetch_ms"],
                            "consumer_status": message_data["consumer"]["status"],
                        }
                        processed_data.append(row)

    return pd.DataFrame(processed_data)

def plot_comparison(df, metric, title, ylabel, output_filename):
    """
    Generates and saves a grouped bar chart for a given metric.
    """
    labels = df['scenario'] + ' ' + df['message_size']
    default_data = df[df['config'] == 'default-cluster-config'][metric]
    benchmark_data = df[df['config'] == 'benchmark-cluster-config'][metric]

    x = np.arange(len(labels.unique()))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 7))
    rects1 = ax.bar(x - width/2, default_data, width, label='Default Config')
    rects2 = ax.bar(x + width/2, benchmark_data, width, label='Benchmark Config')

    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels.unique(), rotation=45, ha="right")
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(round(height, 2)),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset.
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()
    plt.savefig(output_filename)
    plt.close(fig)
    print(f"Generated {output_filename}.")


if __name__ == "__main__":
    results_dir = 'results'
    if not os.path.exists(results_dir):
        print(f"Results directory '{results_dir}' does not exist. Please run the experiments first.")
        exit(1)

    files_to_process = [f for f in os.listdir(results_dir) if f.startswith('comparison_results') and f.endswith('.json')]

    for filename in files_to_process:
        filepath = os.path.join(results_dir, filename)
        with open(filepath, 'r') as f:
            data = json.load(f)

        df = process_data(data)

        if not df.empty:
            base_filename = os.path.splitext(filename)[0]
            # Plot the producer throughput.
            plot_comparison(df, 'producer_throughput_mbps',
                            'Producer Throughput Comparison',
                            'Throughput (MB/s)',
                            f'producer_throughput_{base_filename}.png')

            # Plot the producer latency.
            plot_comparison(df, 'producer_latency_ms',
                            'Producer Latency Comparison',
                            'Latency (ms)',
                            f'producer_latency_{base_filename}.png')

            # Plot the consumer throughput.
            plot_comparison(df, 'consumer_throughput_mbps',
                            'Consumer Throughput Comparison',
                            'Throughput (MB/s)',
                            f'consumer_throughput_{base_filename}.png')

            # Plot the consumer fetch.
            plot_comparison(df, 'consumer_fetch_ms',
                            'Consumer Fetch Time Comparison',
                            'Fetch Time (ms)',
                            f'consumer_fetch_ms_{base_filename}.png')
        else:
            print(f"No valid data to plot in {filename}")
