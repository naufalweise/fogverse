import logging
from typing import Dict, Any

class AnomalyDetector:
    """
    Detects anomalies in incoming metrics. Extend or modify this for more sophisticated logic.
    """

    def __init__(self, anomaly_threshold: float = 2.0):
        self.anomaly_threshold = anomaly_threshold

    def detect_anomaly(self, metrics: Dict[str, Any]) -> bool:
        """
        Compares the 'messages_per_second' metric to the threshold.
        Logs a warning if an anomaly is detected.
        """
        current_value = metrics.get("messages_per_second", 0)
        if current_value > self.anomaly_threshold:
            logging.warning("Anomaly detected: %s exceeds threshold %s", current_value, self.anomaly_threshold)
            return True
        return False
