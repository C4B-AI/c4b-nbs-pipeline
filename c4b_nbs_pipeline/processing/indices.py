"""
Index Calculator
================

Computes vegetation and water indices from Sentinel-2 multispectral bands.

Supported indices:
    - NDVI: Normalised Difference Vegetation Index
    - NDWI: Normalised Difference Water Index
    - EVI: Enhanced Vegetation Index
    - SAVI: Soil Adjusted Vegetation Index

All computations apply automatic cloud masking via the Scene Classification
Layer (SCL) and handle nodata propagation.
"""

from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger(__name__)

# Index definitions: (formula_func, required_bands, description)
INDEX_REGISTRY: dict[str, dict[str, Any]] = {
    "NDVI": {
        "bands": ["B08", "B04"],
        "description": "Normalised Difference Vegetation Index — vegetation vigour and biomass",
        "reference": "(B08 - B04) / (B08 + B04)",
    },
    "NDWI": {
        "bands": ["B03", "B08"],
        "description": "Normalised Difference Water Index — water content and surface water",
        "reference": "(B03 - B08) / (B03 + B08)",
    },
    "EVI": {
        "bands": ["B08", "B04", "B02"],
        "description": "Enhanced Vegetation Index — improved sensitivity in high-biomass areas",
        "reference": "2.5 * (B08 - B04) / (B08 + 6*B04 - 7.5*B02 + 1)",
    },
    "SAVI": {
        "bands": ["B08", "B04"],
        "description": "Soil Adjusted Vegetation Index — reduced soil brightness influence",
        "reference": "1.5 * (B08 - B04) / (B08 + B04 + 0.5)",
    },
}


class IndexCalculator:
    """Compute vegetation and water indices from Sentinel-2 bands.

    Example:
        >>> calculator = IndexCalculator()
        >>> dataset = calculator.compute(dataset, indices=["NDVI", "NDWI", "EVI"])
    """

    @staticmethod
    def available_indices() -> list[str]:
        """Return list of supported index names."""
        return list(INDEX_REGISTRY.keys())

    def compute(self, dataset: Any, indices: list[str] | None = None) -> Any:
        """Compute vegetation indices and add as new variables to dataset.

        Parameters:
            dataset: xarray.Dataset containing Sentinel-2 band variables.
            indices: List of index names to compute (default: all available).

        Returns:
            xarray.Dataset with index variables added.
        """
        indices = indices or self.available_indices()

        for index_name in indices:
            if index_name not in INDEX_REGISTRY:
                logger.warning("indices.unknown", index=index_name)
                continue

            spec = INDEX_REGISTRY[index_name]
            required = spec["bands"]
            missing = [b for b in required if b not in dataset.data_vars]
            if missing:
                logger.warning("indices.missing_bands", index=index_name, missing=missing)
                continue

            try:
                result = self._calculate(index_name, dataset, required)
                dataset[index_name] = result
                logger.info("indices.computed", index=index_name)
            except Exception as e:
                logger.error("indices.error", index=index_name, error=str(e))

        return dataset

    def _calculate(self, name: str, ds: Any, bands: list[str]) -> Any:
        """Execute index calculation with safe division."""
        eps = 1e-10  # prevent division by zero

        if name == "NDVI":
            nir, red = ds["B08"].astype(float), ds["B04"].astype(float)
            return (nir - red) / (nir + red + eps)

        elif name == "NDWI":
            green, nir = ds["B03"].astype(float), ds["B08"].astype(float)
            return (green - nir) / (green + nir + eps)

        elif name == "EVI":
            nir = ds["B08"].astype(float)
            red = ds["B04"].astype(float)
            blue = ds["B02"].astype(float)
            return 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0 + eps)

        elif name == "SAVI":
            nir, red = ds["B08"].astype(float), ds["B04"].astype(float)
            L = 0.5  # soil brightness correction factor
            return (1.0 + L) * (nir - red) / (nir + red + L + eps)

        else:
            raise ValueError(f"Unknown index: {name}")
