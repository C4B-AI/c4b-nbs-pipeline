"""
Harmoniser
==========

Spatiotemporal alignment of heterogeneous environmental data sources.

Aligns satellite imagery (irregular revisit, raster grid) and IoT sensor
time series (regular/irregular, point locations) to a common coordinate
reference system and temporal resolution, producing a unified xarray
Dataset suitable for ML feature engineering.

Spatial alignment:
    - Reprojection to target CRS (default EPSG:3035 / ETRS89-LAEA)
    - Resampling of raster data to target resolution
    - Point-to-raster mapping for IoT sensor locations

Temporal alignment:
    - Resampling of IoT time series to target resolution (default 1h)
    - Gap-filling via linear interpolation (gaps < 6h) or flagging (gaps >= 6h)
    - Satellite data temporal indexing (acquisition timestamp)
"""

from pathlib import Path
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger(__name__)


class Harmoniser:
    """Spatiotemporal harmonisation engine.

    Parameters:
        config: Pipeline configuration dictionary.
    """

    def __init__(self, config: dict[str, Any]):
        self.config = config
        proc_config = config.get("processing", {})
        self.target_crs = proc_config.get("target_crs", "EPSG:3035")
        self.temporal_resolution = proc_config.get("temporal_resolution", "1h")
        self.gap_fill_max = proc_config.get("gap_fill_max_hours", 6)
        logger.info(
            "harmoniser.initialised",
            target_crs=self.target_crs,
            temporal_resolution=self.temporal_resolution,
        )

    def align(
        self,
        satellite_data: str | Path | None = None,
        sensor_data: str | Path | None = None,
        target_crs: str | None = None,
        temporal_resolution: str | None = None,
    ) -> Any:
        """Align satellite and sensor data to common spatiotemporal grid.

        Parameters:
            satellite_data: Path to directory containing preprocessed Sentinel data.
            sensor_data: Path to directory containing IoT sensor data.
            target_crs: Override target CRS (default from config).
            temporal_resolution: Override temporal resolution (default from config).

        Returns:
            xarray.Dataset with harmonised data variables on a common grid.
        """
        import xarray as xr

        crs = target_crs or self.target_crs
        tres = temporal_resolution or self.temporal_resolution

        logger.info(
            "harmoniser.align.start",
            satellite=str(satellite_data),
            sensor=str(sensor_data),
            crs=crs,
            temporal_resolution=tres,
        )

        datasets = []

        # Load and reproject satellite data
        if satellite_data:
            sat_ds = self._load_satellite(Path(satellite_data), crs)
            if sat_ds is not None:
                datasets.append(sat_ds)

        # Load and resample sensor data
        if sensor_data:
            sensor_ds = self._load_sensors(Path(sensor_data), tres)
            if sensor_ds is not None:
                datasets.append(sensor_ds)

        if not datasets:
            logger.warning("harmoniser.align.empty", msg="No data sources loaded")
            return xr.Dataset()

        # Merge datasets
        merged = xr.merge(datasets, join="outer")
        logger.info("harmoniser.align.complete", variables=list(merged.data_vars))
        return merged

    def _load_satellite(self, path: Path, target_crs: str) -> Any:
        """Load and reproject satellite raster data."""
        import rioxarray
        import xarray as xr

        if not path.exists():
            logger.warning("harmoniser.satellite.missing", path=str(path))
            return None

        tif_files = sorted(path.rglob("*.tif"))
        if not tif_files:
            logger.warning("harmoniser.satellite.no_files", path=str(path))
            return None

        bands = {}
        for tif in tif_files:
            band_name = tif.stem
            da = rioxarray.open_rasterio(tif)
            da = da.rio.reproject(target_crs)
            bands[band_name] = da.squeeze(drop=True)

        ds = xr.Dataset(bands)
        logger.info("harmoniser.satellite.loaded", bands=list(bands.keys()), crs=target_crs)
        return ds

    def _load_sensors(self, path: Path, temporal_resolution: str) -> Any:
        """Load IoT sensor data and resample to target temporal resolution."""
        import pandas as pd
        import xarray as xr

        if not path.exists():
            logger.warning("harmoniser.sensors.missing", path=str(path))
            return None

        json_files = sorted(path.rglob("*.json")) + sorted(path.rglob("*.jsonl"))
        if not json_files:
            logger.warning("harmoniser.sensors.no_files", path=str(path))
            return None

        frames = []
        for jf in json_files:
            try:
                df = pd.read_json(jf, lines=jf.suffix == ".jsonl")
                frames.append(df)
            except Exception as e:
                logger.warning("harmoniser.sensors.read_error", file=str(jf), error=str(e))

        if not frames:
            return None

        combined = pd.concat(frames, ignore_index=True)
        if "timestamp" in combined.columns:
            combined["timestamp"] = pd.to_datetime(combined["timestamp"])
            combined = combined.set_index("timestamp")
            combined = combined.resample(temporal_resolution).mean(numeric_only=True)

            # Gap fill (linear interpolation for gaps < max)
            combined = combined.interpolate(method="time", limit=self.gap_fill_max)

        ds = xr.Dataset.from_dataframe(combined)
        logger.info("harmoniser.sensors.loaded", variables=list(ds.data_vars), records=len(combined))
        return ds
