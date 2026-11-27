import logging
from unittest import mock

import pytest

from emf.common.decorators import performance_counter


@pytest.mark.parametrize("units, elapsed, expected_message", [
    ("seconds", 12.3456, "Process 'build_model' finished with duration: 12.35 seconds"),
    ("minutes", 90.0, "Process 'build_model' finished with duration: 1.5 minutes"),
])
def test_performance_counter_returns_result_and_logs_duration(caplog, units, elapsed, expected_message):
    @performance_counter(units=units)
    def build_model(name, version="001"):
        return f"{name}_{version}"

    with mock.patch("emf.common.decorators.time.perf_counter", side_effect=[100.0, 100.0 + elapsed]), \
            caplog.at_level(logging.INFO, logger="emf.common.decorators"):
        result = build_model("CGM", version="002")

    assert result == "CGM_002"
    assert caplog.messages == [expected_message]


def test_performance_counter_keeps_function_metadata():
    @performance_counter()
    def merge_models():
        """Merges models"""

    assert merge_models.__name__ == "merge_models"
    assert merge_models.__doc__ == "Merges models"


def test_performance_counter_propagates_exceptions():
    @performance_counter()
    def failing():
        raise ValueError("load flow diverged")

    with pytest.raises(ValueError, match="load flow diverged"):
        failing()
