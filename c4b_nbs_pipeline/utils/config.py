"""
Configuration Loader
====================

Load and validate pipeline configuration from YAML files with
environment variable substitution.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml
import structlog

logger = structlog.get_logger(__name__)

ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def load_config(path: str | Path) -> dict[str, Any]:
    """Load pipeline configuration from a YAML file.

    Supports environment variable substitution using ${VAR_NAME} syntax.

    Parameters:
        path: Path to the YAML configuration file.

    Returns:
        Configuration dictionary.

    Raises:
        FileNotFoundError: If config file does not exist.
        yaml.YAMLError: If config file contains invalid YAML.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path) as f:
        raw = f.read()

    # Substitute environment variables
    def _replace_env(match):
        var_name = match.group(1)
        value = os.environ.get(var_name, "")
        if not value:
            logger.warning("config.env_var.missing", var=var_name)
        return value

    resolved = ENV_VAR_PATTERN.sub(_replace_env, raw)
    config = yaml.safe_load(resolved)

    logger.info("config.loaded", path=str(path), keys=list(config.keys()) if config else [])
    return config or {}
