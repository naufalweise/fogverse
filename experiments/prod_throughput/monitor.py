from statistics import mean
import subprocess
import time
import os
import select # For non-blocking I/O.
import sys

from experiments.constants import FIRST_CONTAINER
from experiments.prod_throughput.clients import stop_client_data_flow
from fogverse.logger.fog import FogLogger

# Configuration.
UPDATE_INTERVAL = 1.0  # Desired update interval in seconds.
SLIDING_WINDOW_SIZE = 8  # Number of samples to consider for average calculation.

# Thresholds (adjust if needed).
CPU_USAGE_THRESHOLD = 75.0 # in %.
MEM_USAGE_THRESHOLD = 80.0 # in %.
DISK_UTIL_THRESHOLD = 0.92 # in fraction.

# Globals for managing the continuous iostat process and its data.
iostat_process = None
latest_iostat_data = {} # Stores {device_name: util_percent_string}.
current_parsing_block = {} # Holds data for the iostat block being currently parsed.
is_parsing_devices = False # True if current lines are part of a device stats block.

def start_iostat_stream():
    # Starts iostat as a continuous background process.
    global iostat_process
    env = os.environ.copy()
    env['LC_ALL'] = 'C' # Ensures consistent output format for parsing.
    cmd = ['iostat', '-x', '-k', '1'] # Extended stats, kilobytes, 1-second interval.
    iostat_process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, # Capture errors from iostat.
        text=True,
        bufsize=1, # Line-buffered.
        env=env
    )
    if iostat_process.poll() is not None: # Check if process started successfully.
        print(f"ERROR STARTING IOSTAT: {iostat_process.stderr.read()}", file=sys.stderr)
        return False
    return True

def process_iostat_stream():
    # Reads available output from iostat, parses it, and updates disk stats.
    global latest_iostat_data, current_parsing_block, is_parsing_devices

    if not iostat_process or iostat_process.stdout.closed:
        return

    # Process lines from iostat if data is available.
    while select.select([iostat_process.stdout], [], [], 0)[0]:
        line = iostat_process.stdout.readline()
        if not line: # Indicates EOF or that the iostat process may have ended.
            print("iostat stream ended or process exited.", file=sys.stderr)
            if iostat_process: iostat_process.poll() # Update status.
            # Consider logic here to attempt a restart of iostat if desired.
            return

        line_stripped = line.strip()

        if line_stripped.startswith("Device"): # Start of a new device statistics block.
            if is_parsing_devices and current_parsing_block: # Implicit end of a previous block.
                latest_iostat_data = current_parsing_block.copy()
            current_parsing_block.clear()
            is_parsing_devices = True
            continue # Skip the header line itself.

        # An empty line or 'avg-cpu' typically signifies the end of a device block.
        if not line_stripped or line_stripped.startswith("avg-cpu"):
            if is_parsing_devices and current_parsing_block:
                latest_iostat_data = current_parsing_block.copy() # Commit completed block.
            current_parsing_block.clear() # Reset for next potential block.
            is_parsing_devices = False
            continue

        if is_parsing_devices: # If we are in a device block, parse the line.
            parts = line_stripped.split()
            if len(parts) >= 2: # Expect at least device name and %util.
                device_name = parts[0]
                try:
                    percent_util = parts[-1]
                    float(percent_util) # Validate that %util is a number.
                    current_parsing_block[device_name] = percent_util
                except (IndexError, ValueError):
                    pass # Ignore malformed lines within a device stats block.

    # Non-blockingly check iostat's stderr for any error messages.
    if iostat_process.poll() is None and select.select([iostat_process.stderr], [], [], 0)[0]:
        error_output = iostat_process.stderr.readline()
        if error_output:
            # Silently consume or log minimally to avoid cluttering the display.
            # print(f"iostat stderr: {error_output.strip()}", file=sys.stderr)
            pass

def get_docker_stats(container_name_or_id):
    # Fetches CPU and Memory usage for the specified Docker container.
    cmd = ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}\n{{.MemUsage}}", container_name_or_id]
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            return None, None, (stderr.strip() if stderr else "Unknown Docker error.")
    except FileNotFoundError:
        return None, None, "Docker command not found."
    except subprocess.TimeoutExpired:
        return None, None, "Docker stats command timed out."

    lines = stdout.strip().split('\n')
    try:
        cpu_usage = lines[0]
        mem_usage = lines[2]  # mem_usage is confirmed to be at index 2.
    except IndexError:
        return None, None, "Unexpected docker stats output format."

    return cpu_usage, mem_usage, None

def monitor_resource_usage(container_name=FIRST_CONTAINER, show_log=False):
    logger = FogLogger(name=f"resource_usage_{int(time.time())}", csv_header=["timestamp", "cpu_usage", "mem_usage", "disk_util"])
    device = 'dm-0'
    resource_usage = []

    try:
        while True:
            if show_log: print("\033[H\033[J", end="")  # Clear screen.

            loop_start_time = time.monotonic()
            process_iostat_stream()

            cpu_usage, mem_usage, _ = get_docker_stats(container_name)

            if show_log:
                print(f"cpu_usage: {cpu_usage or 'N/A'}")
                print(f"mem_usage: {mem_usage or 'N/A'}")

            disk_util = None
            try:
                disk_util = latest_iostat_data[device]
                if show_log: print(f"Disk util: {disk_util}%")
            except Exception as _:
                if show_log: print("Disk util: N/A")

            now = time.time()

            cpu_usage = float(cpu_usage.strip('%')) if cpu_usage else None
            mem_usage = float(mem_usage.strip('%')) if mem_usage else None
            disk_util = float(disk_util) if disk_util else None

            resource_usage.append((now, cpu_usage, mem_usage, disk_util))

            # Check last few entries.
            window = resource_usage[-SLIDING_WINDOW_SIZE:]
            cpu_usages, mem_usages, disk_utils = zip(*[(c, m, d) for _, c, m, d in window if c and m and d])

            if (
                mean(cpu_usages) >= CPU_USAGE_THRESHOLD or
                mean(mem_usages) >= MEM_USAGE_THRESHOLD or
                mean(disk_utils) >= DISK_UTIL_THRESHOLD * 100  # since disk_util is a %.
            ):
                stop_client_data_flow()
                break  # Exit the loop if threshold exceeded.

            time.sleep(max(0, UPDATE_INTERVAL - (time.monotonic() - loop_start_time)))

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}")
    finally:
        shutdown_iostat_process()

        for _, cpu, mem, disk in resource_usage:
            logger.csv_log(f"{cpu},{mem},{disk}%")

def shutdown_iostat_process():
    # Ensure the iostat subprocess is terminated when the script exits.
    if iostat_process and iostat_process.poll() is None: # Check if process is still running.
        iostat_process.terminate()
        try:
            iostat_process.wait(timeout=1) # Wait briefly for graceful termination.
        except subprocess.TimeoutExpired:
            iostat_process.kill() # Force kill if it doesn't terminate.
        print("iostat process stopped.")

if __name__ == "__main__":
    if not start_iostat_stream():
        sys.exit("Failed to initialize iostat monitoring. Exiting.")
    else:
        monitor_resource_usage(show_log=True)
