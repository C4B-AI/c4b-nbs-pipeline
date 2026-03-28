"""
CLI Entry Point
===============

Command-line interface for the C4B-NbS-Pipeline.

Usage:
    c4b-pipeline ingest sentinel --config config/pipeline.yaml
    c4b-pipeline ingest mqtt --config config/pipeline.yaml
    c4b-pipeline process harmonise --config config/pipeline.yaml
    c4b-pipeline process indices --config config/pipeline.yaml --indices NDVI NDWI
"""

import argparse
import sys

import structlog

from c4b_nbs_pipeline.utils.config import load_config

logger = structlog.get_logger(__name__)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="c4b-pipeline",
        description="C4B-NbS-Pipeline: Copernicus + IoT data ingestion for NbS assessment",
    )
    parser.add_argument("--config", "-c", default="config/pipeline.yaml", help="Path to configuration file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Run data ingestion")
    ingest_parser.add_argument("source", choices=["sentinel", "mqtt", "lorawan", "all"], help="Data source to ingest")

    # Process command
    process_parser = subparsers.add_parser("process", help="Run data processing")
    process_parser.add_argument("operation", choices=["harmonise", "indices", "cloud_mask"], help="Processing operation")
    process_parser.add_argument("--indices", nargs="+", default=None, help="Indices to compute (for 'indices' operation)")

    # Info command
    subparsers.add_parser("info", help="Show pipeline information and configuration")

    args = parser.parse_args()

    if args.verbose:
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(0))

    if not args.command:
        parser.print_help()
        sys.exit(0)

    config = load_config(args.config)

    if args.command == "info":
        _print_info(config)
    elif args.command == "ingest":
        _run_ingest(config, args.source)
    elif args.command == "process":
        _run_process(config, args.operation, args)
    else:
        parser.print_help()


def _print_info(config):
    """Print pipeline configuration summary."""
    from c4b_nbs_pipeline import __version__
    print(f"C4B-NbS-Pipeline v{__version__}")
    print(f"  Data directory: {config.get('output', {}).get('data_dir', 'not configured')}")
    print(f"  Target CRS: {config.get('processing', {}).get('target_crs', 'EPSG:3035')}")
    print(f"  Sentinel collections: {config.get('copernicus', {}).get('collections', [])}")
    print(f"  MQTT broker: {config.get('sensors', {}).get('mqtt', {}).get('broker', 'not configured')}")
    print(f"  LoRaWAN API: {config.get('sensors', {}).get('lorawan', {}).get('chirpstack_api', 'not configured')}")


def _run_ingest(config, source):
    """Execute data ingestion for the specified source."""
    logger.info("cli.ingest.start", source=source)

    if source in ("sentinel", "all"):
        from c4b_nbs_pipeline.ingestion import SentinelIngestor
        ingestor = SentinelIngestor(config)
        scenes = ingestor.search()
        logger.info("cli.ingest.sentinel", scenes_found=len(scenes))
        for scene in scenes:
            ingestor.download(scene)
            ingestor.preprocess(scene)

    if source in ("mqtt", "all"):
        from c4b_nbs_pipeline.ingestion import MQTTIngestor
        ingestor = MQTTIngestor(config)
        ingestor.connect()
        ingestor.start()
        logger.info("cli.ingest.mqtt.running", msg="MQTT ingestion running in background. Press Ctrl+C to stop.")
        try:
            import time
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            ingestor.stop()

    if source in ("lorawan", "all"):
        from c4b_nbs_pipeline.ingestion import LoRaWANIngestor
        ingestor = LoRaWANIngestor(config)
        devices = ingestor.search()
        logger.info("cli.ingest.lorawan", devices_found=len(devices))

    logger.info("cli.ingest.complete", source=source)


def _run_process(config, operation, args):
    """Execute data processing operation."""
    logger.info("cli.process.start", operation=operation)

    if operation == "harmonise":
        from c4b_nbs_pipeline.processing import Harmoniser
        h = Harmoniser(config)
        data_dir = config.get("output", {}).get("data_dir", "/data/nbs-pipeline")
        ds = h.align(satellite_data=f"{data_dir}/sentinel", sensor_data=f"{data_dir}/iot")
        logger.info("cli.process.harmonise.complete", variables=list(ds.data_vars) if ds else [])

    elif operation == "indices":
        from c4b_nbs_pipeline.processing import IndexCalculator
        calc = IndexCalculator()
        indices = args.indices or config.get("processing", {}).get("indices", ["NDVI", "NDWI", "EVI"])
        logger.info("cli.process.indices", indices=indices, msg="Requires pre-harmonised dataset")

    elif operation == "cloud_mask":
        from c4b_nbs_pipeline.processing import CloudMasker
        masker = CloudMasker()
        logger.info("cli.process.cloud_mask", msg="Requires loaded Sentinel-2 dataset")

    logger.info("cli.process.complete", operation=operation)


if __name__ == "__main__":
    main()
