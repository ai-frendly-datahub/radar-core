"""Test that all public exports are importable from radar_core."""

from __future__ import annotations


def test_all_exports_importable() -> None:
    """Verify all exports in __all__ are importable from radar_core."""
    # Import the main module
    import radar_core

    # Get all exported names
    exported_names = radar_core.__all__

    # Verify each name is actually accessible
    for name in exported_names:
        if name == "__version__":
            # Special case: __version__ is a string, not a class/function
            assert hasattr(radar_core, name), f"Missing export: {name}"
            assert isinstance(radar_core.__version__, str)
        else:
            assert hasattr(radar_core, name), f"Missing export: {name}"
            obj = getattr(radar_core, name)
            assert obj is not None, f"Export {name} is None"


def test_new_exports_importable() -> None:
    """Verify new exports are importable."""
    from radar_core import (
        AdaptiveThrottler,
        CrawlHealthAlert,
        CrawlHealthRecord,
        CrawlHealthStore,
        SourceThrottleState,
        TelegramNotifier,
    )

    # Verify classes are importable
    assert AdaptiveThrottler is not None
    assert CrawlHealthAlert is not None
    assert CrawlHealthRecord is not None
    assert CrawlHealthStore is not None
    assert SourceThrottleState is not None
    assert TelegramNotifier is not None
