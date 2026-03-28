# Contributing to C4B-NbS-Pipeline

Thank you for your interest in contributing to the C4B-NbS-Pipeline project.

## How to Contribute

1. **Fork** the repository on GitHub.
2. **Clone** your fork locally: `git clone https://github.com/YOUR_USERNAME/c4b-nbs-pipeline.git`
3. **Create a branch** for your feature or fix: `git checkout -b feature/my-feature`
4. **Install** development dependencies: `pip install -e ".[dev]"`
5. **Write** your code, following the project's coding style (enforced by `ruff`).
6. **Add tests** for new functionality in `tests/`.
7. **Run tests**: `pytest`
8. **Run linting**: `ruff check .`
9. **Commit** with a clear message describing the change.
10. **Push** your branch and open a **Pull Request** against `main`.

## Code Style

- Python 3.10+ type hints throughout.
- PEP 8 compliance enforced via `ruff`.
- Google-style docstrings.
- All public functions and classes must have docstrings.

## Sensor Decoder Plugins

To add support for a new sensor type, implement the `SensorDecoder` protocol (see `c4b_nbs_pipeline/sensors/`) and register it via `MQTTIngestor.register_decoder()` or `LoRaWANIngestor`. See `docs/extending.md` for a step-by-step guide.

## Reporting Issues

Use [GitHub Issues](https://github.com/cloud4business/c4b-nbs-pipeline/issues). Please include:
- Python version and OS
- Steps to reproduce
- Expected vs. actual behaviour
- Relevant log output

## Licence

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
