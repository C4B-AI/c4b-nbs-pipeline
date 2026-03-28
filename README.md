# C4B-NbS-Pipeline

**Open-source data ingestion pipeline for Copernicus Sentinel-1/2 imagery integration with IoT environmental sensor streams.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![DOI](https://img.shields.io/badge/DOI-10.5281%2Fzenodo.XXXXXXX-blue)](https://doi.org/10.5281/zenodo.19278318)
---

## Overview

`c4b-nbs-pipeline` is a modular, open-source Python framework for ingesting, harmonising, and integrating heterogeneous environmental data streams required for evidence-based assessment of Nature-Based Solutions (NbS). The pipeline bridges two critical data domains that are typically processed in isolation:

- **Earth Observation data** from the Copernicus programme (Sentinel-1 SAR and Sentinel-2 multispectral imagery)
- **In-situ IoT sensor streams** transmitted via LoRaWAN and MQTT protocols (water quality, soil moisture, microclimate)

The framework is designed for **on-premise deployment**, ensuring that sensitive environmental, territorial, and financial data remain within the data owner's infrastructure — a core requirement for digital sovereignty in European environmental monitoring contexts.

## Key Features

- **Copernicus Sentinel-1/2 ingestion** via the Copernicus Data Space Ecosystem (CDSE) API, with automated tile selection, download management, and atmospheric correction (Sen2Cor integration)
- **IoT sensor ingestion** supporting LoRaWAN (via ChirpStack) and MQTT (via Mosquitto) protocols with configurable decoders for common environmental sensor payloads
- **Spatiotemporal harmonisation** aligning satellite imagery and sensor time series to a common spatial reference (EPSG:4326/3035) and temporal grid
- **Vegetation index computation** (NDVI, NDWI, EVI, SAVI) from Sentinel-2 bands with cloud masking (SCL-based)
- **Water quality feature engineering** from IoT sensor streams (BOD, COD, TSS, nitrogen, phosphorus) with anomaly detection and gap-filling
- **FAIR-compliant metadata generation** following Dublin Core and ISO 19115 standards
- **Extensible plugin architecture** for adding custom data sources, sensor decoders, and processing modules
- **On-premise first**: no cloud dependencies for core pipeline operations

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    C4B-NbS-Pipeline                      │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Copernicus   │  │  LoRaWAN     │  │  MQTT        │  │
│  │  CDSE API     │  │  ChirpStack  │  │  Mosquitto   │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                  │                  │          │
│         ▼                  ▼                  ▼          │
│  ┌─────────────────────────────────────────────────┐    │
│  │           Ingestion Layer (ingestion/)           │    │
│  │  sentinel.py │ lorawan.py │ mqtt.py │ base.py   │    │
│  └─────────────────────┬───────────────────────────┘    │
│                         │                                │
│                         ▼                                │
│  ┌─────────────────────────────────────────────────┐    │
│  │         Processing Layer (processing/)           │    │
│  │  harmonise.py │ indices.py │ cloud_mask.py      │    │
│  └─────────────────────┬───────────────────────────┘    │
│                         │                                │
│                         ▼                                │
│  ┌─────────────────────────────────────────────────┐    │
│  │           Sensor Layer (sensors/)                │    │
│  │  water_quality.py │ soil.py │ microclimate.py   │    │
│  └─────────────────────┬───────────────────────────┘    │
│                         │                                │
│                         ▼                                │
│  ┌─────────────────────────────────────────────────┐    │
│  │           Utilities (utils/)                     │    │
│  │  metadata.py │ crs.py │ config.py │ logging.py  │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Installation

### Requirements

- Python 3.10+
- GDAL 3.6+ (for geospatial operations)
- Sen2Cor 2.12+ (optional, for Sentinel-2 atmospheric correction)

### From source

```bash
git clone https://github.com/cloud4business/c4b-nbs-pipeline.git
cd c4b-nbs-pipeline
pip install -e ".[dev]"
```

### Dependencies

Core dependencies are listed in `pyproject.toml`. Key packages:

- `rasterio` / `rioxarray` — raster I/O and CRS management
- `xarray` — multidimensional array processing
- `paho-mqtt` — MQTT client
- `chirpstack-api` — LoRaWAN integration
- `pystac-client` — STAC catalogue access for Copernicus CDSE
- `pandas` / `geopandas` — tabular and geospatial data handling
- `numpy` / `scipy` — numerical computation
- `jsonschema` — metadata validation

## Quick Start

### 1. Configure your pipeline

```yaml
# config/pipeline.yaml
copernicus:
  cdse_client_id: "${CDSE_CLIENT_ID}"
  cdse_client_secret: "${CDSE_CLIENT_SECRET}"
  collections:
    - SENTINEL-2
    - SENTINEL-1
  aoi:
    type: "Polygon"
    coordinates: [[[1.8, 41.8], [2.3, 41.8], [2.3, 42.1], [1.8, 42.1], [1.8, 41.8]]]
  max_cloud_cover: 20
  temporal_range: ["2025-01-01", "2025-12-31"]

sensors:
  mqtt:
    broker: "localhost"
    port: 8883
    tls: true
    topics:
      - "nbs/+/water_quality/#"
      - "nbs/+/soil/#"
  lorawan:
    chirpstack_api: "localhost:8080"
    application_id: "nbs-monitoring"

processing:
  target_crs: "EPSG:3035"
  temporal_resolution: "1h"
  indices: ["NDVI", "NDWI", "EVI"]

output:
  data_dir: "/data/nbs-pipeline"
  metadata_standard: "dublin_core"
  format: "zarr"
```

### 2. Run Sentinel-2 ingestion

```python
from c4b_nbs_pipeline.ingestion import SentinelIngestor
from c4b_nbs_pipeline.utils.config import load_config

config = load_config("config/pipeline.yaml")
ingestor = SentinelIngestor(config)

# Search and download available scenes
scenes = ingestor.search()
print(f"Found {len(scenes)} scenes matching criteria")

for scene in scenes:
    ingestor.download(scene)
    ingestor.preprocess(scene)  # cloud masking + atmospheric correction
```

### 3. Ingest IoT sensor data

```python
from c4b_nbs_pipeline.ingestion import MQTTIngestor

mqtt = MQTTIngestor(config)
mqtt.connect()

# Register sensor decoders
mqtt.register_decoder("water_quality", WaterQualityDecoder())
mqtt.register_decoder("soil", SoilMoistureDecoder())

# Start ingestion (runs in background thread)
mqtt.start()
```

### 4. Harmonise and compute indices

```python
from c4b_nbs_pipeline.processing import Harmoniser, IndexCalculator

harmoniser = Harmoniser(config)
dataset = harmoniser.align(
    satellite_data="/data/nbs-pipeline/sentinel2/",
    sensor_data="/data/nbs-pipeline/iot/",
    target_crs="EPSG:3035",
    temporal_resolution="1h"
)

calculator = IndexCalculator()
dataset = calculator.compute(dataset, indices=["NDVI", "NDWI", "EVI"])
dataset.to_zarr("/data/nbs-pipeline/harmonised/")
```

## Supported Sensor Decoders

| Sensor Type | Parameters | Protocol | Decoder Class |
|-------------|-----------|----------|---------------|
| Water quality (multi-probe) | BOD, COD, TSS, NH₄-N, NO₃-N, PO₄-P, pH, DO, EC, turbidity | MQTT / LoRaWAN | `WaterQualityDecoder` |
| Soil moisture | VWC, temperature, EC | LoRaWAN | `SoilMoistureDecoder` |
| Microclimate | Temperature, humidity, pressure, wind, precipitation, solar radiation | LoRaWAN / MQTT | `MicroclimateDecoder` |
| Water level | Stage height, flow rate (derived) | LoRaWAN | `WaterLevelDecoder` |
| Weather station | Full meteorological suite | MQTT | `WeatherStationDecoder` |

Custom decoders can be registered via the plugin interface — see `docs/extending.md`.

## Vegetation Indices

| Index | Formula | Application |
|-------|---------|-------------|
| NDVI | (B08 − B04) / (B08 + B04) | Vegetation vigour and biomass |
| NDWI | (B03 − B08) / (B03 + B08) | Water content / surface water detection |
| EVI | 2.5 × (B08 − B04) / (B08 + 6×B04 − 7.5×B02 + 1) | Enhanced vegetation index (atmospheric correction) |
| SAVI | 1.5 × (B08 − B04) / (B08 + B04 + 0.5) | Soil-adjusted vegetation index |

All indices are computed with automatic cloud masking using the Sentinel-2 Scene Classification Layer (SCL bands 3, 8, 9, 10, 11 masked).

## Data Standards and Interoperability

- **Spatial reference**: All outputs projected to EPSG:3035 (ETRS89-LAEA) by default; configurable target CRS
- **Temporal alignment**: Configurable resolution (default 1h for IoT, native revisit for satellite)
- **Metadata**: Dublin Core and ISO 19115 compliant; FAIR principles applied
- **Output formats**: Zarr (default, cloud-optimised), GeoTIFF, NetCDF, GeoPackage
- **Catalogue**: SpatioTemporal Asset Catalog (STAC) metadata generated for all outputs

## Project Context

This pipeline is developed by [Cloud4Business S.r.l.](https://www.cloud4business.it) as part of its research and innovation activities in on-premise AI infrastructure for environmental applications. The software supports the data ingestion layer of sovereign AI architectures designed for evidence-based NbS assessment, as described in:

- Falcone, S. and Mazzone, M.E. (2026). *On-Premise AI Architectures for Sovereign Environmental Data Processing: Design Principles and ISO 27001 Compliance Framework*. Technical Report. Cloud4Business S.r.l. Zenodo.
- Falcone, S. and Mazzone, M.E. (2026). *Edge-Computing AI Nodes for Real-Time Adaptive Management of Nature-Based Solutions: Architectural Specification and Deployment Protocol*. Technical Specification. Cloud4Business S.r.l. Zenodo.

## Contributing

Contributions are welcome. Please see `CONTRIBUTING.md` for guidelines.

## License

This project is licensed under the Apache License 2.0 — see `LICENSE` for details.

## Citation

If you use this software in your research, please cite:

```bibtex
@software{mazzone_falcone_2026_c4b_nbs_pipeline,
  author       = {Mazzone, Maria Emilia and Falcone, Stefano},
  title        = {C4B-NbS-Pipeline: Open-source data ingestion pipeline
                  for Copernicus Sentinel-1/2 imagery integration with
                  IoT environmental sensor streams},
  year         = {2026},
  publisher    = {Zenodo},
  version      = {v0.1.0},
  doi          = {10.5281/zenodo.XXXXXXX},
  url          = {https://doi.org/10.5281/zenodo.XXXXXXX}
}
```

## Contact

- **Cloud4Business S.r.l.** — [www.cloud4business.it](https://www.cloud4business.it)
- **Issues**: [GitHub Issues](https://github.com/cloud4business/c4b-nbs-pipeline/issues)
