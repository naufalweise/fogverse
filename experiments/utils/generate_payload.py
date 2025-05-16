def generate_payload(logger, filepath='experiments/payload.txt', min_kb = 1, max_kb = 100,step_kb = 1, char = '*'):
    # Generate a payload file with increasing sizes from min_kb to max_kb in steps of step_kb.
    # Each line in the file will contain a string of the specified character repeated to fill the size.
    logger.log_all(f"Generating payload file from {min_kb}KB to {max_kb}KB in steps of {step_kb}KB...")
    with open(filepath, 'w') as f:
        for kb in range(min_kb, max_kb + 1, step_kb):
            payload = char * (kb * 1024)
            f.write(payload + '\n')
    logger.log_all(f"Generated {filepath} successfully.")
