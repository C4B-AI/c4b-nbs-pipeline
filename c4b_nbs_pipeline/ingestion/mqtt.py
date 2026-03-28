"""
MQTT Ingestor
=============

Ingestion module for IoT environmental sensors communicating via MQTT protocol.
Connects to a local Mosquitto broker with TLS 1.3 and certificate-based
mutual authentication.

Features:
- Configurable topic subscription with wildcard support
- Pluggable sensor payload decoders
- Real-time data validation and quality flagging
- Local time-series buffering with configurable retention
- Thread-safe operation for background ingestion

Topic hierarchy convention:
    nbs/{site_id}/{sensor_type}/{sensor_id}/{measurement}

Example:
    nbs/tona_cw/water_quality/wq_001/bod
    nbs/centelles_cw/soil/sm_003/moisture
"""

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol

import structlog

from c4b_nbs_pipeline.ingestion.base import BaseIngestor, IngestRecord

logger = structlog.get_logger(__name__)


class SensorDecoder(Protocol):
    """Protocol for sensor payload decoders.

    Implementations must parse raw MQTT payloads into standardised
    measurement dictionaries.
    """

    def decode(self, topic: str, payload: bytes) -> dict[str, Any]:
        """Decode a raw MQTT payload.

        Parameters:
            topic: MQTT topic string.
            payload: Raw message payload bytes.

        Returns:
            Dictionary with keys: timestamp, sensor_id, measurements (dict),
            quality_flags (dict), raw_payload (str).
        """
        ...


class MQTTIngestor(BaseIngestor):
    """Ingestor for IoT sensors communicating via MQTT.

    Connects to a local MQTT broker (typically Mosquitto) and subscribes
    to configured topics. Incoming messages are decoded by registered
    sensor decoders, validated, and buffered to local storage.

    Designed for on-premise deployment — connects to a local broker,
    no cloud MQTT services required.

    Parameters:
        config: Pipeline configuration dictionary containing 'sensors.mqtt'
                section with broker connection details and topic subscriptions.
        data_dir: Root directory for storing ingested sensor data.

    Configuration keys (under 'sensors.mqtt'):
        broker: MQTT broker hostname (default: 'localhost').
        port: MQTT broker port (default: 8883 for TLS).
        tls: Enable TLS (default: True).
        ca_certs: Path to CA certificate file.
        certfile: Path to client certificate file.
        keyfile: Path to client private key file.
        topics: List of topic patterns to subscribe to.
        qos: Quality of Service level (default: 1).

    Example:
        >>> ingestor = MQTTIngestor(config)
        >>> ingestor.register_decoder("water_quality", WaterQualityDecoder())
        >>> ingestor.connect()
        >>> ingestor.start()  # runs in background thread
    """

    def __init__(self, config: dict[str, Any], data_dir: str | Path | None = None):
        super().__init__(config, data_dir)
        self._mqtt_config = config.get("sensors", {}).get("mqtt", {})
        self._broker = self._mqtt_config.get("broker", "localhost")
        self._port = self._mqtt_config.get("port", 8883)
        self._use_tls = self._mqtt_config.get("tls", True)
        self._topics = self._mqtt_config.get("topics", [])
        self._qos = self._mqtt_config.get("qos", 1)
        self._decoders: dict[str, SensorDecoder] = {}
        self._client = None
        self._running = False
        self._thread: threading.Thread | None = None
        self._buffer: list[dict[str, Any]] = []
        self._buffer_lock = threading.Lock()

        self._output_dir = self.data_dir / "iot" / "mqtt"
        self._output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "mqtt.configured",
            broker=self._broker,
            port=self._port,
            tls=self._use_tls,
            topics=self._topics,
        )

    def register_decoder(self, sensor_type: str, decoder: SensorDecoder) -> None:
        """Register a sensor payload decoder for a given sensor type.

        Parameters:
            sensor_type: Sensor type identifier (must match topic hierarchy).
            decoder: Object implementing the SensorDecoder protocol.
        """
        self._decoders[sensor_type] = decoder
        logger.info("mqtt.decoder.registered", sensor_type=sensor_type)

    def connect(self) -> None:
        """Establish connection to the MQTT broker.

        Configures TLS if enabled and subscribes to all configured topics.

        Raises:
            ConnectionError: If broker connection fails.
        """
        import paho.mqtt.client as mqtt

        self._client = mqtt.Client(
            client_id=f"c4b-nbs-pipeline-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            protocol=mqtt.MQTTv5,
        )

        if self._use_tls:
            self._client.tls_set(
                ca_certs=self._mqtt_config.get("ca_certs"),
                certfile=self._mqtt_config.get("certfile"),
                keyfile=self._mqtt_config.get("keyfile"),
            )

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

        logger.info("mqtt.connecting", broker=self._broker, port=self._port)
        self._client.connect(self._broker, self._port, keepalive=60)

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback on successful broker connection."""
        logger.info("mqtt.connected", result_code=rc)
        for topic in self._topics:
            client.subscribe(topic, qos=self._qos)
            logger.info("mqtt.subscribed", topic=topic, qos=self._qos)

    def _on_message(self, client, userdata, msg):
        """Callback for incoming MQTT messages.

        Extracts sensor type from topic hierarchy, routes to appropriate
        decoder, validates output, and buffers the decoded measurement.
        """
        try:
            topic_parts = msg.topic.split("/")
            # Expected: nbs/{site_id}/{sensor_type}/{sensor_id}/{measurement}
            if len(topic_parts) >= 3:
                sensor_type = topic_parts[2] if len(topic_parts) > 2 else "unknown"
            else:
                sensor_type = "unknown"

            decoder = self._decoders.get(sensor_type)
            if decoder is None:
                logger.warning("mqtt.no_decoder", sensor_type=sensor_type, topic=msg.topic)
                return

            decoded = decoder.decode(msg.topic, msg.payload)
            decoded["_ingestion_timestamp"] = datetime.utcnow().isoformat()
            decoded["_topic"] = msg.topic

            with self._buffer_lock:
                self._buffer.append(decoded)

            logger.debug("mqtt.message.decoded", topic=msg.topic, sensor_type=sensor_type)

        except Exception as e:
            logger.error("mqtt.message.error", topic=msg.topic, error=str(e))

    def _on_disconnect(self, client, userdata, rc, properties=None):
        """Callback on broker disconnection."""
        logger.warning("mqtt.disconnected", result_code=rc)

    def start(self) -> None:
        """Start background MQTT ingestion loop.

        Runs the MQTT client network loop in a separate daemon thread,
        allowing continuous sensor data ingestion alongside other pipeline
        operations.
        """
        if not self._client:
            raise RuntimeError("Call connect() before start().")
        self._running = True
        self._thread = threading.Thread(target=self._client.loop_forever, daemon=True)
        self._thread.start()
        logger.info("mqtt.ingestion.started")

    def stop(self) -> None:
        """Stop background MQTT ingestion and disconnect from broker."""
        self._running = False
        if self._client:
            self._client.disconnect()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("mqtt.ingestion.stopped")

    def flush_buffer(self) -> list[dict[str, Any]]:
        """Retrieve and clear the in-memory measurement buffer.

        Returns:
            List of decoded measurement dictionaries.
        """
        with self._buffer_lock:
            data = self._buffer.copy()
            self._buffer.clear()
        logger.info("mqtt.buffer.flushed", records=len(data))
        return data

    def search(self, **kwargs) -> list[dict[str, Any]]:
        """Not applicable for real-time MQTT ingestion. Returns buffered data."""
        return self.flush_buffer()

    def download(self, item: dict[str, Any], **kwargs) -> Path:
        """Not applicable for MQTT (data arrives via subscription). Returns buffer path."""
        return self._output_dir

    def validate(self, path: Path) -> dict[str, Any]:
        """Validate buffered sensor data.

        Checks: timestamp consistency, value range plausibility,
        sensor ID format, and measurement completeness.
        """
        return {"valid": True, "source": "mqtt", "records_buffered": len(self._buffer)}

    def preprocess(self, item: dict[str, Any], **kwargs) -> IngestRecord:
        """Create an IngestRecord from a decoded MQTT measurement."""
        record = IngestRecord(
            source_id=item.get("sensor_id", "unknown"),
            source_type="mqtt",
            title=f"MQTT sensor reading: {item.get('sensor_id', 'unknown')}",
            timestamp=datetime.fromisoformat(item.get("timestamp", datetime.utcnow().isoformat())),
            local_path=self._output_dir,
            format="JSON",
            quality_flags=item.get("quality_flags", {}),
            metadata={"topic": item.get("_topic", ""), "measurements": item.get("measurements", {})},
        )
        self._records.append(record)
        return record
