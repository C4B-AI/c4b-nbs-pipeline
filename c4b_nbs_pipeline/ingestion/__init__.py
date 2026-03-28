"""
Ingestion Layer
===============

Data acquisition modules for heterogeneous environmental data sources.

Supported sources:
- Copernicus Sentinel-1/2 via CDSE API (SentinelIngestor)
- IoT sensors via MQTT protocol (MQTTIngestor)
- IoT sensors via LoRaWAN / ChirpStack (LoRaWANIngestor)
"""

from c4b_nbs_pipeline.ingestion.base import BaseIngestor
from c4b_nbs_pipeline.ingestion.sentinel import SentinelIngestor
from c4b_nbs_pipeline.ingestion.mqtt import MQTTIngestor
from c4b_nbs_pipeline.ingestion.lorawan import LoRaWANIngestor

__all__ = ["BaseIngestor", "SentinelIngestor", "MQTTIngestor", "LoRaWANIngestor"]
