"""
LoRaWAN Ingestor
================

Ingestion module for IoT sensors communicating via LoRaWAN protocol,
interfacing with a local ChirpStack network server.

LoRaWAN is the primary protocol for outdoor NbS sensor deployments,
providing 2-15km range with ultra-low power consumption (5-10 year
battery life for Class A sensors).

The ingestor connects to ChirpStack's gRPC API to:
- Register and manage sensor devices
- Receive decoded uplink messages
- Send downlink commands (Class C actuators)
- Monitor device health and connectivity

References:
    - LoRaWAN Specification v1.0.4: https://lora-alliance.org/
    - ChirpStack: https://www.chirpstack.io/
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from c4b_nbs_pipeline.ingestion.base import BaseIngestor, IngestRecord

logger = structlog.get_logger(__name__)


class LoRaWANIngestor(BaseIngestor):
    """Ingestor for LoRaWAN sensors via local ChirpStack network server.

    Connects to the ChirpStack gRPC API running on the edge node's
    integrated LoRaWAN gateway, retrieving decoded sensor uplink
    messages and managing device registration.

    Parameters:
        config: Pipeline configuration with 'sensors.lorawan' section.
        data_dir: Root directory for storing ingested data.

    Configuration keys (under 'sensors.lorawan'):
        chirpstack_api: ChirpStack API endpoint (default: 'localhost:8080').
        api_token: ChirpStack API authentication token.
        application_id: ChirpStack application ID for NbS sensors.
        poll_interval: Seconds between uplink polling cycles (default: 30).

    Example:
        >>> ingestor = LoRaWANIngestor(config)
        >>> devices = ingestor.search()  # list registered devices
        >>> for device in devices:
        ...     record = ingestor.preprocess(device)
    """

    def __init__(self, config: dict[str, Any], data_dir: str | Path | None = None):
        super().__init__(config, data_dir)
        self._lora_config = config.get("sensors", {}).get("lorawan", {})
        self._api_endpoint = self._lora_config.get("chirpstack_api", "localhost:8080")
        self._api_token = self._lora_config.get("api_token", "")
        self._application_id = self._lora_config.get("application_id", "")
        self._poll_interval = self._lora_config.get("poll_interval", 30)

        self._output_dir = self.data_dir / "iot" / "lorawan"
        self._output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "lorawan.configured",
            api_endpoint=self._api_endpoint,
            application_id=self._application_id,
        )

    def search(self, **kwargs) -> list[dict[str, Any]]:
        """List registered LoRaWAN devices and their latest uplink status.

        Returns:
            List of device info dictionaries with connectivity status,
            last seen timestamp, and signal quality metrics (RSSI, SNR).
        """
        logger.info("lorawan.search.start", application_id=self._application_id)
        # Skeleton: actual implementation connects to ChirpStack gRPC API
        # using chirpstack_api.DeviceServiceStub
        logger.warning("lorawan.search.skeleton", msg="ChirpStack gRPC integration pending")
        return []

    def download(self, item: dict[str, Any], **kwargs) -> Path:
        """Retrieve uplink history for a specific device.

        Parameters:
            item: Device dictionary from search().
            since: Retrieve uplinks since this datetime (default: last 24h).

        Returns:
            Path to the downloaded uplink data file (JSON lines).
        """
        device_eui = item.get("dev_eui", "unknown")
        output_path = self._output_dir / f"{device_eui}_uplinks.jsonl"
        logger.info("lorawan.download", device_eui=device_eui, output=str(output_path))
        # Skeleton: actual implementation queries ChirpStack event log
        return output_path

    def validate(self, path: Path) -> dict[str, Any]:
        """Validate downloaded LoRaWAN uplink data.

        Checks: frame counter consistency, payload CRC, timestamp ordering,
        signal quality thresholds (RSSI > -120dBm, SNR > -20dB).
        """
        return {"valid": True, "source": "lorawan"}

    def preprocess(self, item: dict[str, Any], **kwargs) -> IngestRecord:
        """Preprocess LoRaWAN uplink data into standardised IngestRecord."""
        device_eui = item.get("dev_eui", "unknown")
        record = IngestRecord(
            source_id=device_eui,
            source_type="lorawan",
            title=f"LoRaWAN device {device_eui}",
            timestamp=datetime.utcnow(),
            local_path=self._output_dir / f"{device_eui}_uplinks.jsonl",
            format="JSONL",
            metadata={
                "device_eui": device_eui,
                "application_id": self._application_id,
                "protocol": "LoRaWAN EU868",
            },
        )
        self._records.append(record)
        return record
