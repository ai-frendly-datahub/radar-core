from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from radar_core.crawl_health import CrawlHealthStore


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "radar_data.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE marker (id INTEGER)")
    conn.execute("INSERT INTO marker VALUES (1)")
    conn.close()
    return db_path


def test_safe_connect_happy_path_no_extra_io(tmp_path: Path) -> None:
    db_path = _make_db(tmp_path)
    wal_before = sorted(p.name for p in tmp_path.glob("*.wal*"))

    conn = CrawlHealthStore._safe_connect(db_path)
    try:
        assert isinstance(conn, duckdb.DuckDBPyConnection)
    finally:
        conn.close()

    wal_after = sorted(p.name for p in tmp_path.glob("*.wal*"))
    assert wal_after == wal_before, "Happy path must not touch WAL files"


def test_safe_connect_quarantines_corrupt_wal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _make_db(tmp_path)
    wal_path = Path(str(db_path) + ".wal")
    wal_path.write_bytes(b"\x00\x01\x02CORRUPT_WAL_BLOB\xff")
    original_wal_bytes = wal_path.read_bytes()

    real_connect = duckdb.connect
    calls = {"n": 0}

    def fake_connect(arg: object, *args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        calls["n"] += 1
        if calls["n"] == 1:
            raise duckdb.InternalException(
                "INTERNAL Error: Failure while replaying WAL file (test)"
            )
        return real_connect(arg, *args, **kwargs)  # type: ignore[arg-type]

    with patch("radar_core.crawl_health.duckdb.connect", side_effect=fake_connect):
        conn = CrawlHealthStore._safe_connect(db_path)
    try:
        assert calls["n"] == 2, "Expected exactly two connect attempts"
    finally:
        conn.close()

    assert not wal_path.exists(), "Original WAL must be moved out of the way"
    quarantined = sorted(tmp_path.glob("*.wal.broken-*"))
    assert len(quarantined) == 1, f"Expected 1 quarantined WAL, found {quarantined}"
    assert quarantined[0].read_bytes() == original_wal_bytes, (
        "Quarantined WAL must preserve original bytes for inspection"
    )

    err = capsys.readouterr().err
    assert "WAL quarantined" in err
    assert str(wal_path) in err
    assert str(quarantined[0]) in err


def test_safe_connect_no_wal_present_reraises_original(tmp_path: Path) -> None:
    db_path = tmp_path / "radar_data.duckdb"
    sentinel = duckdb.InternalException("INTERNAL Error: synthetic without WAL")

    def fake_connect(arg: object, *args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        raise sentinel

    with patch("radar_core.crawl_health.duckdb.connect", side_effect=fake_connect):
        with pytest.raises(duckdb.InternalException) as exc_info:
            CrawlHealthStore._safe_connect(db_path)
    assert exc_info.value is sentinel, "Original exception must be propagated unchanged"


def test_safe_connect_double_failure_reraises_original(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = _make_db(tmp_path)
    wal_path = Path(str(db_path) + ".wal")
    wal_path.write_bytes(b"CORRUPT")

    original = duckdb.InternalException("INTERNAL Error: first attempt fails")
    secondary = duckdb.InternalException("INTERNAL Error: second attempt also fails")
    calls = {"n": 0}

    def fake_connect(arg: object, *args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        calls["n"] += 1
        if calls["n"] == 1:
            raise original
        raise secondary

    with patch("radar_core.crawl_health.duckdb.connect", side_effect=fake_connect):
        with pytest.raises(duckdb.InternalException) as exc_info:
            CrawlHealthStore._safe_connect(db_path)

    assert exc_info.value is original, (
        "When the retry also fails, the ORIGINAL exception must surface "
        "so operators see the root cause"
    )
    assert calls["n"] == 2
    quarantined = sorted(tmp_path.glob("*.wal.broken-*"))
    assert len(quarantined) == 1, "WAL must still be quarantined for inspection"
    assert "WAL quarantined" in capsys.readouterr().err


def test_safe_connect_quarantine_rename_failure_preserves_wal(
    tmp_path: Path,
) -> None:
    db_path = _make_db(tmp_path)
    wal_path = Path(str(db_path) + ".wal")
    wal_path.write_bytes(b"CORRUPT")

    original = duckdb.InternalException("INTERNAL Error: connect failed")
    rename_error = OSError("rename forbidden by test")

    def fake_connect(arg: object, *args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        raise original

    def fake_rename(self: Path, target: object) -> None:
        raise rename_error

    with patch("radar_core.crawl_health.duckdb.connect", side_effect=fake_connect), \
         patch.object(Path, "rename", fake_rename):
        with pytest.raises(duckdb.InternalException) as exc_info:
            CrawlHealthStore._safe_connect(db_path)

    assert exc_info.value is original
    assert wal_path.exists(), "WAL must remain in place when quarantine fails"


def test_store_init_uses_safe_connect_end_to_end(tmp_path: Path) -> None:
    db_path = _make_db(tmp_path)
    wal_path = Path(str(db_path) + ".wal")
    wal_path.write_bytes(b"\x00CORRUPT_WAL\xff")

    real_connect = duckdb.connect
    calls = {"n": 0}

    def fake_connect(arg: object, *args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        calls["n"] += 1
        if calls["n"] == 1:
            raise duckdb.InternalException(
                "INTERNAL Error: Failure while replaying WAL file (e2e)"
            )
        return real_connect(arg, *args, **kwargs)  # type: ignore[arg-type]

    with patch("radar_core.crawl_health.duckdb.connect", side_effect=fake_connect):
        store = CrawlHealthStore(str(db_path))
    try:
        assert isinstance(store.conn, duckdb.DuckDBPyConnection)
    finally:
        store.close()

    assert not wal_path.exists()
    assert len(list(tmp_path.glob("*.wal.broken-*"))) == 1
