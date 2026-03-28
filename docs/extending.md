# Extending C4B-NbS-Pipeline

## Adding a Custom Sensor Decoder

The pipeline uses a plugin architecture for sensor payload decoding. To add support for a new sensor type, implement the `SensorDecoder` protocol and register it with the appropriate ingestor.

### Step 1: Create the decoder

Create a new file in `c4b_nbs_pipeline/sensors/`, e.g., `my_sensor.py`:

```python
import json
from datetime import datetime
from typing import Any

PARAMETER_RANGES = {
    "my_param": (0.0, 100.0, "unit"),
}


class MySensorDecoder:
    """Decoder for My Custom Sensor payloads."""

    def decode(self, topic: str, payload: bytes) -> dict[str, Any]:
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}

        sensor_id = data.get("sensor_id", "unknown")
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
            "sensor_type": "my_sensor",
            "measurements": measurements,
            "quality_flags": quality_flags,
            "raw_payload": payload.decode("utf-8", errors="replace"),
        }
```

### Step 2: Register the decoder

```python
from c4b_nbs_pipeline.ingestion import MQTTIngestor
from c4b_nbs_pipeline.sensors.my_sensor import MySensorDecoder

mqtt = MQTTIngestor(config)
mqtt.register_decoder("my_sensor", MySensorDecoder())
mqtt.connect()
mqtt.start()
```

### Step 3: Configure MQTT topics

In `config/pipeline.yaml`, add the topic pattern:

```yaml
sensors:
  mqtt:
    topics:
      - "nbs/+/my_sensor/#"
```

## Adding a Custom Data Source

To add a new data ingestion source (e.g., a new satellite platform or data API), subclass `BaseIngestor` and implement the four abstract methods: `search()`, `download()`, `validate()`, `preprocess()`.

See the existing implementations in `c4b_nbs_pipeline/ingestion/` for reference patterns.

## Adding a Custom Vegetation Index

Edit `c4b_nbs_pipeline/processing/indices.py` and add an entry to `INDEX_REGISTRY`:

```python
INDEX_REGISTRY["MY_INDEX"] = {
    "bands": ["B08", "B04"],
    "description": "My custom index",
    "reference": "(B08 - B04) / (B08 + B04 + 0.1)",
}
```

Then add the computation logic in `IndexCalculator._calculate()`.
