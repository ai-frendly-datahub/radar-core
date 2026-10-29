#!/usr/bin/env python3
"""KST/UTC published-date backfill. Dry-run unless ``--apply`` is passed.

레포의 DuckDB ``articles`` 와 ``data/**/*.jsonl`` 을 읽어 소스별 건수를 출력한다.
``published_raw`` 가 없거나, 저장된 시각과 올바른 시각의 차이가 정확히 9시간이
아니면 고치지 않는다. ``--apply`` 없이 실행하면 어떤 파일도 수정하지 않는다.

예::

    python scripts/backfill_published_timezone.py --repos-root ../radars
    python scripts/backfill_published_timezone.py --repo ../GameRadar --apply

이 스크립트를 실제 Radar 체크아웃에 대해 실행하지 말고, 각 레포 담당자가
문서를 검토한 뒤 직접 실행한다. 자세한 조건은 ``docs/timezone-backfill.md``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from radar_core.timezone_backfill import format_report, scan_repositories


def _repos_from_args(repos_root: Path | None, repos: list[Path]) -> list[Path]:
    found = list(repos)
    if repos_root is None:
        return found
    root = repos_root.expanduser()
    if (root / "data").is_dir() or (root / "config").is_dir():
        found.append(root)
        return found
    if not root.is_dir():
        found.append(root)
        return found
    for child in sorted(root.iterdir()):
        if child.is_dir() and not child.name.startswith("."):
            found.append(child)
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "DuckDB/jsonl published 시각의 KST·UTC 9시간 오차를 집계한다. "
            "기본값은 dry-run 이며 --apply 가 있을 때만 수정한다."
        )
    )
    parser.add_argument(
        "--repos-root",
        type=Path,
        help="Radar 레포들이 들어 있는 디렉터리, 또는 레포 루트",
    )
    parser.add_argument(
        "--repo",
        action="append",
        default=[],
        type=Path,
        help="검사할 레포 루트. 여러 번 지정할 수 있다.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="조건을 만족하는 행만 수정한다. 이 플래그가 없으면 읽기만 한다.",
    )
    args = parser.parse_args(argv)
    repos = _repos_from_args(args.repos_root, args.repo)
    if not repos:
        parser.error("--repos-root 또는 --repo 가 필요합니다.")

    apply = bool(args.apply)
    result = scan_repositories(repos, apply=apply)
    sys.stdout.write(format_report(result, apply=apply))
    if result.errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
