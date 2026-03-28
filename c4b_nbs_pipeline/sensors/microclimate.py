"""
Microclimate Decoder
====================

Decodes payloads from microclimate stations deployed at NbS sites.
Parameters: temperature, humidity, pressure, wind speed/direction,
precipitation, solar radiation.
"""

import json
from datetime import datetime
from typing import Any

PARAMETER_RANGES = {
    "temperature": (-40.0, 60.0, "°C"),
    "humidity": (0.0, 100.0, "%"),
    "pressure": (800.0, 1100.0, "hPa"),
    "wind_speed": (0.0, 75.0, "m/s"),
    "wind_direction": (0.0, 360.0, "°"),
    "precipitation": (0.0, 500.0, "mm/h"),
    "solar_radiation": (0.0, 1500.0, "W/m²"),
}


class MicroclimateDecoder:
    """Decoder for microclimate station payloads."""

    def decode(self, topic: str, payload: bytes) -> dict[str, Any]:
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}

        sensor_id = data.get("sensor_id", topic.split("/")[3] if len(topic.split("/")) > 3 else "unknown")
        measurements = {}
        quality_flags = {}

        for param, (min_val, max_val, unit) in PARAMETER_RANGES.items():
            if param in data:
                value = float(data[param])
                measurements[param] = {"value": value, "unit": unit}
                if not (min_val <= value <= max_val):
                    quality_flags[param] = "out_of_range"

        return {
            "sensor_id": sensor_id,
            "timestamp": data.get("timestamp", datetime.utcnow().isoformat()),
            "sensor_type": "microclimate",
            "measurements": measurements,
            "quality_flags": quality_flags,
            "raw_payload": payload.decode("utf-8", errors="replace"),
        }
