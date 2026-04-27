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

<!-- DATAHUB-OPS-AUDIT:START -->
## DataHub Operations

- CI/CD workflows: `publish-package.yml`.
- GitHub Pages visualization: `reports/index.html` (valid HTML); no Pages deployment workflow detected.
- Latest remote Pages check: not applicable.
- Local workspace audit: 57 Python files parsed, 0 syntax errors.
- Re-run audit from the workspace root: `python scripts/audit_ci_pages_readme.py --syntax-check --write`.
- Latest audit report: `_workspace/2026-04-14_github_ci_pages_readme_audit.md`.
- Latest Pages URL report: `_workspace/2026-04-14_github_pages_url_check.md`.
<!-- DATAHUB-OPS-AUDIT:END -->
