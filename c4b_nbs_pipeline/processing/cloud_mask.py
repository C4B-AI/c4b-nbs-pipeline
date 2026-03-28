"""
Cloud Masker
============

Applies cloud and shadow masking to Sentinel-2 imagery using the
Scene Classification Layer (SCL).

SCL classes masked by default:
    3 — Cloud shadows
    8 — Cloud medium probability
    9 — Cloud high probability
   10 — Thin cirrus
   11 — Snow/ice (configurable)
"""

from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger(__name__)

# SCL classes to mask (default configuration)
DEFAULT_MASK_CLASSES = [3, 8, 9, 10, 11]


class CloudMasker:
    """Cloud and shadow masking for Sentinel-2 data.

    Parameters:
        mask_classes: List of SCL class values to mask.
            Default: [3, 8, 9, 10, 11] (shadows, clouds, cirrus, snow).
        scl_variable: Name of the SCL variable in the dataset.
    """

    def __init__(
        self,
        mask_classes: list[int] | None = None,
        scl_variable: str = "SCL",
    ):
        self.mask_classes = mask_classes or DEFAULT_MASK_CLASSES
        self.scl_variable = scl_variable

    def apply(self, dataset: Any, inplace: bool = False) -> Any:
        """Apply cloud mask to all spectral bands in the dataset.

        Parameters:
            dataset: xarray.Dataset containing spectral bands and SCL.
            inplace: If True, modify dataset in place. If False, return copy.

        Returns:
            Masked xarray.Dataset with cloudy pixels set to NaN.
        """
        if self.scl_variable not in dataset.data_vars:
            logger.warning("cloud_mask.no_scl", msg="SCL band not found; skipping cloud masking")
            return dataset

        scl = dataset[self.scl_variable]
        mask = np.isin(scl.values, self.mask_classes)
        cloud_fraction = float(np.sum(mask)) / mask.size

        logger.info(
            "cloud_mask.apply",
            mask_classes=self.mask_classes,
            cloud_fraction=round(cloud_fraction, 4),
        )

        ds = dataset if inplace else dataset.copy(deep=True)

        for var_name in ds.data_vars:
            if var_name == self.scl_variable:
                continue
            if ds[var_name].dims == scl.dims:
                ds[var_name] = ds[var_name].where(~mask)

        ds.attrs["cloud_fraction"] = cloud_fraction
        ds.attrs["cloud_mask_classes"] = self.mask_classes
        return ds

    def compute_cloud_fraction(self, dataset: Any) -> float:
        """Compute cloud fraction without applying mask.

        Returns:
            Float between 0.0 and 1.0 representing cloudy pixel fraction.
        """
        if self.scl_variable not in dataset.data_vars:
            return 0.0
        scl = dataset[self.scl_variable]
        mask = np.isin(scl.values, self.mask_classes)
        return float(np.sum(mask)) / mask.size
