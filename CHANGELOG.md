# Changelog

## Unreleased

- `generate_summary_json` takes an optional `run_collection`. When it is set, the summary JSON gains a `run` object (this execution's per-source counts, sanitized failures, and last success times) and a `sources_window` label that marks `sources` as the rolling window, defaulting to 7 days. The window counts themselves are unchanged. Omitting the argument leaves the summary JSON identical to the previous output.
- `generate_report` forwards that same optional argument. Existing callers do not need to change.

## v0.2.0

- Adds ontology runtime-contract helpers, summary metadata emission, DuckDB ontology backfill, and opt-in event model payload helpers.
- Preserves legacy summary JSON output unless callers explicitly attach per-article `event_model_payload` data.
- Exports the new ontology helpers through the package public API.

## v0.1.0

- Initial extraction of shared core modules from `Radar-Template/radar/`.
- Includes storage, collector, analyzer, models, resilience, raw logging, search indexing, and common validators/quality checks.
- Adds unit test suite for core storage/analyzer/collector/raw logger behavior.
