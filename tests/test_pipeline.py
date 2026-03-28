"""
Test Suite for C4B-NbS-Pipeline
================================

Unit tests for sensor decoders, configuration loading, metadata generation,
and index computation.
"""

import json
import pytest
from pathlib import Path
from datetime import datetime


# ============================================================
# Sensor Decoder Tests
# ============================================================

class TestWaterQualityDecoder:
    """Tests for WaterQualityDecoder."""

    def setup_method(self):
        from c4b_nbs_pipeline.sensors.water_quality import WaterQualityDecoder
        self.decoder = WaterQualityDecoder()

    def test_decode_valid_json(self):
        payload = json.dumps({
            "sensor_id": "wq_001",
            "timestamp": "2026-03-15T10:30:00Z",
            "bod": 12.5,
            "cod": 45.2,
            "tss": 18.3,
            "ph": 7.2,
        }).encode()
        result = self.decoder.decode("nbs/tona_cw/water_quality/wq_001/multi", payload)

        assert result["sensor_id"] == "wq_001"
        assert result["sensor_type"] == "water_quality"
        assert "bod" in result["measurements"]
        assert result["measurements"]["bod"]["value"] == 12.5
        assert result["measurements"]["bod"]["unit"] == "mg/L"

    def test_decode_out_of_range_flags(self):
        payload = json.dumps({
            "sensor_id": "wq_002",
            "ph": 15.0,  # out of range (0-14)
        }).encode()
        result = self.decoder.decode("nbs/site/water_quality/wq_002/ph", payload)

        assert "ph" in result["quality_flags"]
        assert result["quality_flags"]["ph"] == "out_of_range"

    def test_decode_regulatory_exceedance(self):
        payload = json.dumps({
            "sensor_id": "wq_003",
            "bod": 30.0,  # exceeds 25 mg/L regulatory limit
        }).encode()
        result = self.decoder.decode("nbs/site/water_quality/wq_003/bod", payload)

        assert result["quality_flags"]["bod"] == "exceeds_regulatory_limit"

    def test_extract_sensor_id_from_topic(self):
        payload = json.dumps({"bod": 10.0}).encode()
        result = self.decoder.decode("nbs/site_a/water_quality/wq_099/bod", payload)
        assert result["sensor_id"] == "wq_099"


class TestSoilMoistureDecoder:
    """Tests for SoilMoistureDecoder."""

    def setup_method(self):
        from c4b_nbs_pipeline.sensors.soil import SoilMoistureDecoder
        self.decoder = SoilMoistureDecoder()

    def test_decode_valid_payload(self):
        payload = json.dumps({
            "sensor_id": "sm_001",
            "vwc": 35.2,
            "temperature": 18.5,
            "conductivity": 450.0,
        }).encode()
        result = self.decoder.decode("nbs/site/soil/sm_001/multi", payload)

        assert result["sensor_type"] == "soil"
        assert result["measurements"]["vwc"]["value"] == 35.2
        assert result["measurements"]["vwc"]["unit"] == "%"

    def test_decode_out_of_range(self):
        payload = json.dumps({"vwc": 150.0}).encode()  # max is 100%
        result = self.decoder.decode("nbs/site/soil/sm_002/vwc", payload)
        assert result["quality_flags"]["vwc"] == "out_of_range"


class TestMicroclimateDecoder:
    """Tests for MicroclimateDecoder."""

    def setup_method(self):
        from c4b_nbs_pipeline.sensors.microclimate import MicroclimateDecoder
        self.decoder = MicroclimateDecoder()

    def test_decode_valid_payload(self):
        payload = json.dumps({
            "sensor_id": "mc_001",
            "temperature": 22.3,
            "humidity": 65.0,
            "pressure": 1013.25,
            "solar_radiation": 850.0,
        }).encode()
        result = self.decoder.decode("nbs/site/microclimate/mc_001/multi", payload)

        assert result["sensor_type"] == "microclimate"
        assert len(result["measurements"]) == 4


# ============================================================
# Configuration Tests
# ============================================================

class TestConfig:
    """Tests for configuration loading."""

    def test_load_config(self, tmp_path):
        from c4b_nbs_pipeline.utils.config import load_config

        config_content = """
copernicus:
  max_cloud_cover: 15
processing:
  target_crs: "EPSG:3035"
output:
  data_dir: "/tmp/test"
"""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        config = load_config(config_file)
        assert config["copernicus"]["max_cloud_cover"] == 15
        assert config["processing"]["target_crs"] == "EPSG:3035"

    def test_missing_config_raises(self):
        from c4b_nbs_pipeline.utils.config import load_config
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path.yaml")


# ============================================================
# Metadata Tests
# ============================================================

class TestMetadata:
    """Tests for Dublin Core metadata generation."""

    def test_generate_basic_metadata(self):
        from c4b_nbs_pipeline.utils.metadata import generate_dublin_core

        meta = generate_dublin_core(
            title="Test Dataset",
            creator="Test Author",
            description="A test dataset for unit testing.",
            subject=["test", "NbS"],
        )

        assert meta["dc:title"] == "Test Dataset"
        assert meta["dc:creator"] == ["Test Author"]
        assert meta["dc:publisher"] == "Cloud4Business S.r.l."
        assert meta["dc:rights"] == "CC-BY 4.0"
        assert "test" in meta["dc:subject"]

    def test_multiple_creators(self):
        from c4b_nbs_pipeline.utils.metadata import generate_dublin_core

        meta = generate_dublin_core(
            title="Multi-author",
            creator=["Author A", "Author B"],
        )
        assert len(meta["dc:creator"]) == 2


# ============================================================
# Index Calculator Tests
# ============================================================

class TestIndexCalculator:
    """Tests for vegetation index computation."""

    def test_available_indices(self):
        from c4b_nbs_pipeline.processing.indices import IndexCalculator
        calc = IndexCalculator()
        indices = calc.available_indices()
        assert "NDVI" in indices
        assert "NDWI" in indices
        assert "EVI" in indices
        assert "SAVI" in indices

    def test_ndvi_computation(self):
        """Test NDVI = (NIR - Red) / (NIR + Red)."""
        import numpy as np

        try:
            import xarray as xr
        except ImportError:
            pytest.skip("xarray not installed")

        from c4b_nbs_pipeline.processing.indices import IndexCalculator

        nir = np.array([[0.5, 0.6], [0.7, 0.8]])
        red = np.array([[0.1, 0.2], [0.1, 0.1]])

        ds = xr.Dataset({
            "B08": (["y", "x"], nir),
            "B04": (["y", "x"], red),
        })

        calc = IndexCalculator()
        result = calc.compute(ds, indices=["NDVI"])

        assert "NDVI" in result.data_vars
        # NDVI for (0.5, 0.1) = 0.4/0.6 ≈ 0.667
        expected = (nir - red) / (nir + red + 1e-10)
        np.testing.assert_array_almost_equal(result["NDVI"].values, expected, decimal=4)


# ============================================================
# Cloud Masker Tests
# ============================================================

class TestCloudMasker:
    """Tests for cloud masking."""

    def test_cloud_fraction(self):
        import numpy as np

        try:
            import xarray as xr
        except ImportError:
            pytest.skip("xarray not installed")

        from c4b_nbs_pipeline.processing.cloud_mask import CloudMasker

        # SCL with 25% cloud (class 9)
        scl = np.array([[4, 4, 9, 9], [4, 4, 4, 4]])
        ds = xr.Dataset({"SCL": (["y", "x"], scl)})

        masker = CloudMasker()
        frac = masker.compute_cloud_fraction(ds)
        assert abs(frac - 0.25) < 0.01
