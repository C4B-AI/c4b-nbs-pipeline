"""
Example: Constructed Wetland Monitoring Pipeline
=================================================

This example demonstrates how to set up a complete data ingestion
pipeline for monitoring a constructed wetland NbS installation,
combining Copernicus Sentinel-2 imagery with on-site IoT water
quality sensors.

Requirements:
    - CDSE account credentials (set CDSE_CLIENT_ID and CDSE_CLIENT_SECRET)
    - Local MQTT broker running (e.g., Mosquitto)
    - c4b-nbs-pipeline installed

Usage:
    python examples/wetland_monitoring.py --config config/pipeline.yaml
"""

import argparse
import time

from c4b_nbs_pipeline.utils.config import load_config
from c4b_nbs_pipeline.ingestion import SentinelIngestor, MQTTIngestor
from c4b_nbs_pipeline.sensors import WaterQualityDecoder, MicroclimateDecoder
from c4b_nbs_pipeline.processing import Harmoniser, IndexCalculator, CloudMasker


def main():
    parser = argparse.ArgumentParser(description="Constructed wetland monitoring example")
    parser.add_argument("--config", "-c", default="config/pipeline.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    print("=" * 60)
    print("C4B-NbS-Pipeline — Constructed Wetland Example")
    print("=" * 60)

    # --- Step 1: Ingest Sentinel-2 imagery ---
    print("\n[1/5] Searching Copernicus CDSE for Sentinel-2 scenes...")
    sentinel = SentinelIngestor(config)
    try:
        scenes = sentinel.search()
        print(f"      Found {len(scenes)} scenes matching criteria.")
        for scene in scenes[:3]:  # download first 3 for demo
            sentinel.download(scene)
            sentinel.preprocess(scene)
    except Exception as e:
        print(f"      Sentinel ingestion skipped: {e}")

    # --- Step 2: Start IoT sensor ingestion ---
    print("\n[2/5] Connecting to MQTT broker for IoT sensor data...")
    mqtt = MQTTIngestor(config)
    mqtt.register_decoder("water_quality", WaterQualityDecoder())
    mqtt.register_decoder("microclimate", MicroclimateDecoder())
    try:
        mqtt.connect()
        mqtt.start()
        print("      MQTT ingestion running. Collecting for 30 seconds...")
        time.sleep(30)
        mqtt.stop()
        sensor_data = mqtt.flush_buffer()
        print(f"      Collected {len(sensor_data)} sensor readings.")
    except Exception as e:
        print(f"      MQTT ingestion skipped: {e}")

    # --- Step 3: Harmonise data ---
    print("\n[3/5] Harmonising satellite and sensor data...")
    harmoniser = Harmoniser(config)
    data_dir = config.get("output", {}).get("data_dir", "/data/nbs-pipeline")
    try:
        dataset = harmoniser.align(
            satellite_data=f"{data_dir}/sentinel",
            sensor_data=f"{data_dir}/iot",
        )
        print(f"      Harmonised dataset: {list(dataset.data_vars)}")
    except Exception as e:
        print(f"      Harmonisation skipped: {e}")
        return

    # --- Step 4: Cloud masking ---
    print("\n[4/5] Applying cloud mask...")
    masker = CloudMasker()
    dataset = masker.apply(dataset)
    cloud_frac = dataset.attrs.get("cloud_fraction", "N/A")
    print(f"      Cloud fraction: {cloud_frac}")

    # --- Step 5: Compute vegetation indices ---
    print("\n[5/5] Computing vegetation indices...")
    calculator = IndexCalculator()
    dataset = calculator.compute(dataset, indices=["NDVI", "NDWI", "EVI"])
    print(f"      Final dataset variables: {list(dataset.data_vars)}")

    # --- Save output ---
    output_path = f"{data_dir}/harmonised/wetland_example.zarr"
    print(f"\n      Saving to {output_path}...")
    try:
        dataset.to_zarr(output_path, mode="w")
        print("      Done.")
    except Exception as e:
        print(f"      Save skipped: {e}")

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
