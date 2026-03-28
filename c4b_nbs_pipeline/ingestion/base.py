"""
Base Ingestor
=============

Abstract base class for all data ingestion modules. Defines the common
interface for search, download, validate, and preprocess operations.

All ingestors produce FAIR-compliant metadata alongside ingested data,
following Dublin Core and ISO 19115 standards.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class IngestRecord:
    """Metadata record for an ingested data item.

    Follows Dublin Core metadata element set with extensions for
    geospatial and temporal coverage (ISO 19115).

    Attributes:
        source_id: Unique identifier from the source system.
        source_type: Data source category (e.g., 'sentinel-2', 'mqtt', 'lorawan').
        title: Human-readable title for the data item.
        timestamp: Acquisition timestamp (UTC).
        bbox: Geographic bounding box [west, south, east, north] in EPSG:4326.
        crs: Coordinate reference system (EPSG code).
        local_path: Path to the downloaded/ingested data on local storage.
        format: Data format (e.g., 'GeoTIFF', 'JSON', 'Zarr').
        quality_flags: Dictionary of quality assessment results.
        metadata: Additional Dublin Core / ISO 19115 metadata fields.
        checksum: SHA-256 hash of the ingested data file.
    """

    source_id: str
    source_type: str
    title: str
    timestamp: datetime
    bbox: list[float] | None = None
    crs: str = "EPSG:4326"
    local_path: Path | None = None
    format: str = ""
    quality_flags: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    checksum: str = ""


class BaseIngestor(ABC):
    """Abstract base class for data ingestion modules.

    Each concrete ingestor implements the four-stage pipeline:
    search → download → validate → preprocess

    The base class provides common functionality for configuration
    management, logging, metadata generation, and error handling.

    Parameters:
        config: Pipeline configuration dictionary (from YAML).
        data_dir: Root directory for storing ingested data.
    """

    def __init__(self, config: dict[str, Any], data_dir: str | Path | None = None):
        self.config = config
        self.data_dir = Path(data_dir or config.get("output", {}).get("data_dir", "/data/nbs-pipeline"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._records: list[IngestRecord] = []
        logger.info("ingestor.initialised", ingestor=self.__class__.__name__, data_dir=str(self.data_dir))

    @abstractmethod
    def search(self, **kwargs) -> list[dict[str, Any]]:
        """Search for available data items matching the configured criteria.

        Returns:
            List of search result dictionaries with source-specific metadata.
        """
        ...

    @abstractmethod
    def download(self, item: dict[str, Any], **kwargs) -> Path:
        """Download a single data item to local storage.

        Parameters:
            item: Search result dictionary from search().

        Returns:
            Path to the downloaded file.
        """
        ...

    @abstractmethod
    def validate(self, path: Path) -> dict[str, Any]:
        """Validate an ingested data item for quality and completeness.

        Parameters:
            path: Path to the downloaded data file.

        Returns:
            Dictionary of quality flags and validation results.
        """
        ...

    @abstractmethod
    def preprocess(self, item: dict[str, Any], **kwargs) -> IngestRecord:
        """Preprocess an ingested data item (format conversion, CRS alignment, etc.).

        Parameters:
            item: Search result dictionary with local_path populated.

        Returns:
            IngestRecord with full metadata.
        """
        ...

    def get_records(self) -> list[IngestRecord]:
        """Return all ingestion records from this session."""
        return self._records.copy()

    def _generate_checksum(self, path: Path) -> str:
        """Compute SHA-256 checksum of a file for integrity verification."""
        import hashlib

        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
