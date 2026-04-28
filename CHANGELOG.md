# Changelog

## v0.2.0

- Adds ontology runtime-contract helpers, summary metadata emission, DuckDB ontology backfill, and opt-in event model payload helpers.
- Preserves legacy summary JSON output unless callers explicitly attach per-article `event_model_payload` data.
- Exports the new ontology helpers through the package public API.

## v0.1.0

- Initial extraction of shared core modules from `Radar-Template/radar/`.
- Includes storage, collector, analyzer, models, resilience, raw logging, search indexing, and common validators/quality checks.
- Adds unit test suite for core storage/analyzer/collector/raw logger behavior.
