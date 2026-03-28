"""
Processing Layer
================

Spatiotemporal harmonisation and feature engineering modules.

- Harmoniser: Aligns satellite and IoT data to common CRS and temporal grid
- IndexCalculator: Computes vegetation indices from Sentinel-2 bands
- CloudMasker: Applies SCL-based cloud masking to Sentinel-2 imagery
"""

from c4b_nbs_pipeline.processing.harmonise import Harmoniser
from c4b_nbs_pipeline.processing.indices import IndexCalculator
from c4b_nbs_pipeline.processing.cloud_mask import CloudMasker

__all__ = ["Harmoniser", "IndexCalculator", "CloudMasker"]
