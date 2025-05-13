import subprocess
import time
import os
import select # For non-blocking I/O.
import sys

# Configuration.
CONTAINER_NAME = "test-container-0"
TARGET_INTERVAL = 1.0  # Desired refresh interval in seconds.

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
        print(f"Error starting iostat: {iostat_process.stderr.read()}", file=sys.stderr)
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
    
    output = stdout.strip().split('\n')
    cpu = output[0] if len(output) > 0 else "N/A"
    mem = output[1] if len(output) > 1 else "N/A"
    return cpu, mem, None

if __name__ == "__main__":
    if not start_iostat_stream():
        sys.exit("Failed to initialize iostat monitoring. Exiting.")

    try:
        while True:
            loop_start_time = time.monotonic()

            process_iostat_stream() # Update disk I/O data from the stream.

            # Clear the terminal screen.
            print("\033[H\033[J", end="")

            # Get and display Docker container statistics.
            cpu_usage, mem_usage, docker_error = get_docker_stats(CONTAINER_NAME)
            if docker_error:
                print(f"CPU Usage: Error ({docker_error})")
                print(f"RAM Usage: Error ({docker_error})")
            else:
                print(f"CPU Usage: {cpu_usage}")
                print(f"RAM Usage: {mem_usage}")
            
            # Display the latest disk I/O statistics.
            if latest_iostat_data:
                for device, util in sorted(latest_iostat_data.items()): # Sort for consistent order.
                    print(f"disk {device} %util: {util}%")
            else:
                print("Disk %util: Awaiting data from iostat.") # Initial state or if iostat fails.
            
            # Calculate elapsed time and sleep to maintain the target refresh interval.
            loop_end_time = time.monotonic()
            elapsed_time = loop_end_time - loop_start_time
            sleep_duration = TARGET_INTERVAL - elapsed_time
            if sleep_duration > 0:
                time.sleep(sleep_duration)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        # Ensure the iostat subprocess is terminated when the script exits.
        if iostat_process and iostat_process.poll() is None: # Check if process is still running.
            iostat_process.terminate()
            try:
                iostat_process.wait(timeout=1) # Wait briefly for graceful termination.
            except subprocess.TimeoutExpired:
                iostat_process.kill() # Force kill if it doesn't terminate.
            print("iostat process stopped.")