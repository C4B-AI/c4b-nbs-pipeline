"""
Water Quality Decoder
=====================

Decodes payloads from multi-parameter water quality probes monitoring
NbS performance (constructed wetlands, bioswales, retention ponds).

Supported parameters:
    BOD (mg/L), COD (mg/L), TSS (mg/L), NH4-N (mg/L), NO3-N (mg/L),
    PO4-P (mg/L), pH, dissolved oxygen (mg/L), electrical conductivity
    (μS/cm), turbidity (NTU), temperature (°C).

Validation ranges are based on typical NbS monitoring contexts and
EU regulatory thresholds (Directive 2024/3019).
"""

import json
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# Plausibility ranges for water quality parameters
# (min, max, unit, regulatory_limit_urban_ww)
PARAMETER_RANGES: dict[str, tuple[float, float, str, float | None]] = {
    "bod": (0.0, 500.0, "mg/L", 25.0),
    "cod": (0.0, 2000.0, "mg/L", 125.0),
    "tss": (0.0, 1000.0, "mg/L", 35.0),
    "nh4_n": (0.0, 100.0, "mg/L", None),
    "no3_n": (0.0, 100.0, "mg/L", None),
    "po4_p": (0.0, 50.0, "mg/L", None),
    "ph": (0.0, 14.0, "-", None),
    "dissolved_oxygen": (0.0, 20.0, "mg/L", None),
    "conductivity": (0.0, 10000.0, "μS/cm", None),
    "turbidity": (0.0, 4000.0, "NTU", None),
    "temperature": (-5.0, 50.0, "°C", None),
}


class WaterQualityDecoder:
    """Decoder for multi-parameter water quality sensor payloads.

    Expects JSON payloads with the following structure:
    {
        "sensor_id": "wq_001",
        "timestamp": "2026-03-15T10:30:00Z",
        "bod": 12.5,
        "cod": 45.2,
        "tss": 18.3,
        "ph": 7.2,
        ...
    }

    LoRaWAN payloads may be binary-encoded; override _decode_binary()
    for device-specific binary formats.
    """

    def decode(self, topic: str, payload: bytes) -> dict[str, Any]:
        """Decode water quality sensor payload.

        Parameters:
            topic: MQTT topic or LoRaWAN application/device path.
            payload: Raw payload bytes (JSON or binary).

        Returns:
            Standardised measurement dictionary with quality flags.
        """
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = self._decode_binary(payload)

        sensor_id = data.get("sensor_id", self._extract_sensor_id(topic))
        timestamp = data.get("timestamp", datetime.utcnow().isoformat())

        measurements = {}
        quality_flags = {}

        for param, (min_val, max_val, unit, reg_limit) in PARAMETER_RANGES.items():
            if param in data:
                value = float(data[param])
                measurements[param] = {"value": value, "unit": unit}

                # Range validation
                if not (min_val <= value <= max_val):
                    quality_flags[param] = "out_of_range"
                    logger.warning(
                        "wq.out_of_range",
                        sensor=sensor_id,
                        param=param,
                        value=value,
                        range=(min_val, max_val),
                    )
                elif reg_limit is not None and value > reg_limit:
                    quality_flags[param] = "exceeds_regulatory_limit"

        return {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "sensor_type": "water_quality",
            "measurements": measurements,
            "quality_flags": quality_flags,
            "raw_payload": payload.decode("utf-8", errors="replace"),
        }

    def _decode_binary(self, payload: bytes) -> dict[str, Any]:
        """Decode binary LoRaWAN payload. Override for device-specific formats."""
        logger.warning("wq.binary_decode.not_implemented")
        return {"sensor_id": "unknown", "timestamp": datetime.utcnow().isoformat()}

    @staticmethod
    def _extract_sensor_id(topic: str) -> str:
        """Extract sensor ID from MQTT topic hierarchy."""
        parts = topic.split("/")
        return parts[3] if len(parts) > 3 else "unknown"
