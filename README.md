# radar-core

Shared Python package for the AI-Friendly DataHub Radar ecosystem.

## What it provides

- shared storage, search, analyzer, migration, and raw logging utilities
- common validation and quality-check helpers
- optional Korean keyword matching via `kiwipiepy`

## Install

### Git tag install (recommended first rollout)

```bash
pip install "radar-core @ git+https://github.com/AI-Friendly-DataHub/radar-core.git@v0.2.0"
```

### With Korean support

```bash
pip install "radar-core[korean] @ git+https://github.com/AI-Friendly-DataHub/radar-core.git@v0.2.0"
```

## Local development

```bash
python3.11 -m pip install -e .[dev]
python3.11 -m pytest tests/ -q
```

## Release flow

1. update version in `pyproject.toml`
2. create a git tag like `v0.2.1`
3. push the tag
4. let GitHub Actions build and publish the wheel/sdist artifact

The included workflow is designed for GitHub Packages publishing and can also be adapted to PyPI later.
