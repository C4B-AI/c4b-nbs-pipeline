"""
Soil Moisture Decoder
=====================

Decodes payloads from soil moisture sensors deployed in NbS contexts
(bioswales, rain gardens, constructed wetlands, riparian buffers).

Parameters: volumetric water content (VWC), soil temperature, electrical conductivity.
"""

import json
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

PARAMETER_RANGES = {
    "vwc": (0.0, 100.0, "%"),         # Volumetric Water Content
    "temperature": (-20.0, 60.0, "°C"),
    "conductivity": (0.0, 5000.0, "μS/cm"),
}


class SoilMoistureDecoder:
    """Decoder for soil moisture sensor payloads."""

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
            "sensor_type": "soil",
            "measurements": measurements,
            "quality_flags": quality_flags,
            "raw_payload": payload.decode("utf-8", errors="replace"),
        }
