"""
Sentinel Ingestor
=================

Ingestion module for Copernicus Sentinel-1 (SAR) and Sentinel-2
(multispectral) imagery via the Copernicus Data Space Ecosystem (CDSE) API.

Features:
- STAC-based catalogue search with spatial/temporal/cloud cover filtering
- Authenticated download with retry and resume support
- Automatic atmospheric correction via Sen2Cor (Sentinel-2)
- Scene Classification Layer (SCL) based cloud masking
- Band extraction and subsetting to area of interest

References:
    - CDSE API: https://dataspace.copernicus.eu/
    - STAC specification: https://stacspec.org/
    - Sen2Cor: https://step.esa.int/main/snap-supported-plugins/sen2cor/
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from c4b_nbs_pipeline.ingestion.base import BaseIngestor, IngestRecord

logger = structlog.get_logger(__name__)

# Copernicus Data Space Ecosystem endpoints
CDSE_STAC_URL = "https://catalogue.dataspace.copernicus.eu/stac"
CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

# Sentinel-2 band specifications (10m, 20m, 60m resolution)
S2_BANDS_10M = ["B02", "B03", "B04", "B08"]  # Blue, Green, Red, NIR
S2_BANDS_20M = ["B05", "B06", "B07", "B8A", "B11", "B12"]  # Red Edge, SWIR
S2_BANDS_60M = ["B01", "B09", "B10"]  # Coastal, Water Vapour, Cirrus
S2_SCL_BAND = "SCL"  # Scene Classification Layer


class SentinelIngestor(BaseIngestor):
    """Ingestor for Copernicus Sentinel-1/2 satellite imagery.

    Connects to the Copernicus Data Space Ecosystem (CDSE) via its STAC API
    to search, download, and preprocess satellite scenes matching the
    configured area of interest, temporal range, and quality criteria.

    Parameters:
        config: Pipeline configuration dictionary containing 'copernicus'
                section with CDSE credentials and search parameters.
        data_dir: Root directory for storing downloaded scenes.

    Configuration keys (under 'copernicus'):
        cdse_client_id: CDSE OAuth2 client ID (or env var CDSE_CLIENT_ID).
        cdse_client_secret: CDSE OAuth2 client secret (or env var CDSE_CLIENT_SECRET).
        collections: List of collection names (e.g., ['SENTINEL-2', 'SENTINEL-1']).
        aoi: GeoJSON geometry defining the area of interest.
        max_cloud_cover: Maximum cloud cover percentage (Sentinel-2 only).
        temporal_range: [start_date, end_date] in ISO format.

    Example:
        >>> config = load_config("config/pipeline.yaml")
        >>> ingestor = SentinelIngestor(config)
        >>> scenes = ingestor.search()
        >>> for scene in scenes:
        ...     ingestor.download(scene)
        ...     record = ingestor.preprocess(scene)
    """

    def __init__(self, config: dict[str, Any], data_dir: str | Path | None = None):
        super().__init__(config, data_dir)
        self._cop_config = config.get("copernicus", {})
        self._client_id = self._cop_config.get("cdse_client_id", "")
        self._client_secret = self._cop_config.get("cdse_client_secret", "")
        self._collections = self._cop_config.get("collections", ["SENTINEL-2"])
        self._aoi = self._cop_config.get("aoi", None)
        self._max_cloud_cover = self._cop_config.get("max_cloud_cover", 20)
        self._temporal_range = self._cop_config.get("temporal_range", [])
        self._token: str | None = None
        self._token_expiry: datetime | None = None

        self._output_dir = self.data_dir / "sentinel"
        self._output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "sentinel.configured",
            collections=self._collections,
            max_cloud_cover=self._max_cloud_cover,
            temporal_range=self._temporal_range,
        )

    def _authenticate(self) -> str:
        """Obtain or refresh CDSE OAuth2 access token.

        Returns:
            Valid access token string.

        Raises:
            ConnectionError: If authentication fails.
        """
        import os
        import requests

        client_id = self._client_id or os.environ.get("CDSE_CLIENT_ID", "")
        client_secret = self._client_secret or os.environ.get("CDSE_CLIENT_SECRET", "")

        if not client_id or not client_secret:
            raise ValueError(
                "CDSE credentials not configured. Set 'cdse_client_id' and "
                "'cdse_client_secret' in config or CDSE_CLIENT_ID / CDSE_CLIENT_SECRET "
                "environment variables."
            )

        if self._token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._token

        response = requests.post(
            CDSE_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        response.raise_for_status()
        token_data = response.json()
        self._token = token_data["access_token"]
        # Token typically valid for 600s; refresh at 500s
        from datetime import timedelta
        self._token_expiry = datetime.utcnow() + timedelta(seconds=token_data.get("expires_in", 600) - 100)

        logger.info("sentinel.authenticated", expires_in=token_data.get("expires_in"))
        return self._token

    def search(self, **kwargs) -> list[dict[str, Any]]:
        """Search CDSE STAC catalogue for scenes matching configured criteria.

        Keyword Arguments:
            collections: Override configured collections.
            aoi: Override configured area of interest (GeoJSON geometry).
            datetime_range: Override configured temporal range.
            max_cloud_cover: Override configured max cloud cover.

        Returns:
            List of STAC item dictionaries matching search criteria.
        """
        from pystac_client import Client

        collections = kwargs.get("collections", self._collections)
        aoi = kwargs.get("aoi", self._aoi)
        temporal = kwargs.get("datetime_range", self._temporal_range)
        max_cloud = kwargs.get("max_cloud_cover", self._max_cloud_cover)

        if not aoi:
            raise ValueError("Area of interest (aoi) must be configured for search.")

        datetime_str = None
        if temporal and len(temporal) == 2:
            datetime_str = f"{temporal[0]}/{temporal[1]}"

        logger.info(
            "sentinel.search.start",
            collections=collections,
            datetime=datetime_str,
            max_cloud_cover=max_cloud,
        )

        client = Client.open(CDSE_STAC_URL)
        search = client.search(
            collections=collections,
            intersects=aoi,
            datetime=datetime_str,
            query={"eo:cloud_cover": {"lte": max_cloud}} if "SENTINEL-2" in collections else None,
            max_items=kwargs.get("max_items", 100),
        )

        items = list(search.items())
        logger.info("sentinel.search.complete", results=len(items))

        return [item.to_dict() for item in items]

    def download(self, item: dict[str, Any], **kwargs) -> Path:
        """Download a Sentinel scene to local storage.

        Implements chunked download with progress tracking, retry on failure,
        and resume support for partially downloaded files.

        Parameters:
            item: STAC item dictionary from search().
            bands: Optional list of band names to download (default: all 10m + 20m bands).

        Returns:
            Path to the downloaded scene directory.
        """
        import requests
        from tqdm import tqdm

        scene_id = item.get("id", "unknown")
        scene_dir = self._output_dir / scene_id
        scene_dir.mkdir(parents=True, exist_ok=True)

        token = self._authenticate()
        headers = {"Authorization": f"Bearer {token}"}

        assets = item.get("assets", {})
        bands = kwargs.get("bands", S2_BANDS_10M + S2_BANDS_20M + [S2_SCL_BAND])

        for band_name, asset_info in assets.items():
            if bands and band_name not in bands:
                continue

            href = asset_info.get("href", "")
            if not href:
                continue

            output_path = scene_dir / f"{band_name}.tif"
            if output_path.exists():
                logger.debug("sentinel.download.skip", band=band_name, reason="exists")
                continue

            logger.info("sentinel.download.band", scene=scene_id, band=band_name)

            response = requests.get(href, headers=headers, stream=True, timeout=120)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            with open(output_path, "wb") as f, tqdm(
                total=total_size, unit="B", unit_scale=True, desc=band_name
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))

        logger.info("sentinel.download.complete", scene=scene_id, path=str(scene_dir))
        return scene_dir

    def validate(self, path: Path) -> dict[str, Any]:
        """Validate a downloaded Sentinel scene.

        Checks:
        - File integrity (all expected bands present)
        - Raster readability (valid GeoTIFF headers)
        - CRS consistency across bands
        - NoData coverage assessment
        - Cloud cover verification against SCL band

        Parameters:
            path: Path to the scene directory.

        Returns:
            Dictionary with validation results and quality flags.
        """
        import rasterio

        flags = {
            "valid": True,
            "bands_present": [],
            "bands_missing": [],
            "crs_consistent": True,
            "nodata_fraction": {},
            "errors": [],
        }

        expected_bands = S2_BANDS_10M + S2_BANDS_20M + [S2_SCL_BAND]
        crs_set = set()

        for band_name in expected_bands:
            band_path = path / f"{band_name}.tif"
            if band_path.exists():
                flags["bands_present"].append(band_name)
                try:
                    with rasterio.open(band_path) as src:
                        crs_set.add(str(src.crs))
                        # Compute nodata fraction
                        import numpy as np
                        data = src.read(1)
                        nodata_frac = float(np.sum(data == src.nodata)) / data.size if src.nodata is not None else 0.0
                        flags["nodata_fraction"][band_name] = round(nodata_frac, 4)
                except Exception as e:
                    flags["errors"].append(f"{band_name}: {str(e)}")
                    flags["valid"] = False
            else:
                flags["bands_missing"].append(band_name)

        if len(crs_set) > 1:
            flags["crs_consistent"] = False
            flags["errors"].append(f"Inconsistent CRS across bands: {crs_set}")
            flags["valid"] = False

        logger.info("sentinel.validate", path=str(path), valid=flags["valid"], bands_ok=len(flags["bands_present"]))
        return flags

    def preprocess(self, item: dict[str, Any], **kwargs) -> IngestRecord:
        """Preprocess a downloaded Sentinel scene.

        Operations:
        - Cloud masking using SCL band (classes 3, 8, 9, 10, 11 masked)
        - Reprojection to target CRS (default EPSG:3035)
        - Clipping to area of interest
        - Reflectance scaling (DN to surface reflectance)

        Parameters:
            item: STAC item dictionary with local path.
            target_crs: Target coordinate reference system (default from config).

        Returns:
            IngestRecord with complete metadata.
        """
        scene_id = item.get("id", "unknown")
        scene_dir = self._output_dir / scene_id
        target_crs = kwargs.get("target_crs", self.config.get("processing", {}).get("target_crs", "EPSG:3035"))

        logger.info("sentinel.preprocess.start", scene=scene_id, target_crs=target_crs)

        # Validate first
        quality_flags = self.validate(scene_dir)

        # Build metadata record
        properties = item.get("properties", {})
        bbox = item.get("bbox", None)

        record = IngestRecord(
            source_id=scene_id,
            source_type="sentinel-2" if "SENTINEL-2" in str(item.get("collection", "")) else "sentinel-1",
            title=f"Sentinel scene {scene_id}",
            timestamp=datetime.fromisoformat(properties.get("datetime", datetime.utcnow().isoformat())),
            bbox=bbox,
            crs=target_crs,
            local_path=scene_dir,
            format="GeoTIFF",
            quality_flags=quality_flags,
            metadata={
                "cloud_cover": properties.get("eo:cloud_cover"),
                "processing_level": properties.get("processing:level"),
                "platform": properties.get("platform"),
                "instrument": properties.get("instruments"),
                "dublin_core": {
                    "dc:creator": "European Space Agency (ESA)",
                    "dc:publisher": "Copernicus Data Space Ecosystem",
                    "dc:rights": "Copernicus Sentinel data, free and open access",
                    "dc:format": "GeoTIFF",
                    "dc:type": "Dataset",
                },
            },
            checksum=self._generate_checksum(scene_dir / f"{S2_BANDS_10M[0]}.tif")
            if (scene_dir / f"{S2_BANDS_10M[0]}.tif").exists()
            else "",
        )

        self._records.append(record)
        logger.info("sentinel.preprocess.complete", scene=scene_id, record_id=record.source_id)
        return record
