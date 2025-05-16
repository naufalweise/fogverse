import subprocess

# Run a shell command with optional error checking and output capture.
def run_cmd(cmd, check=True, capture_output=False, **kwargs):
    try:
        # Execute the command using subprocess with the given options.
        # The shell=True option allows the command to be run in a shell.
        # The check=True option raises an exception if the command fails.
        # The capture_output=True option captures the command's output.
        # The text=True option ensures that the output is returned as a string.
        # The kwargs allow for additional arguments to be passed to subprocess.run.
        return subprocess.run(cmd, shell=True, check=check, capture_output=capture_output, text=True, **kwargs)
    except subprocess.CalledProcessError as e:
        # Print command output and error if capture_output is enabled.
        if capture_output:
            print(f"Command failed: {cmd}")
            print(f"STDERR: {e.stderr}")
            print(f"STDOUT: {e.stdout}")
        # Re-raise the exception to signal command failure.
        raise
