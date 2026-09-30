from datetime import datetime, timedelta

import pytest
from pytz import timezone

from emf.common.helpers import time as time_helper
from emf.common.helpers.time import (convert_to_timezone, convert_to_utc, parse_datetime, parse_duration,
                                     reference_times)

BRUSSELS = timezone("Europe/Brussels")
UTC = timezone("UTC")


@pytest.mark.parametrize("reference, date_time, expected", [
    ("currentMinuteStart", datetime(2025, 8, 14, 13, 47, 25, 123456), datetime(2025, 8, 14, 13, 47)),
    ("currentHourStart", datetime(2025, 8, 14, 13, 47, 25, 123456), datetime(2025, 8, 14, 13)),
    ("currentDayStart", datetime(2025, 8, 14, 13, 47, 25, 123456), datetime(2025, 8, 14)),
    ("currentWeekStart", datetime(2025, 8, 14, 13, 47, 25, 123456), datetime(2025, 8, 11)),
    ("currentWeekStart", datetime(2025, 8, 11, 0, 0), datetime(2025, 8, 11)),
    ("currentWeekStart", datetime(2025, 8, 17, 23, 59), datetime(2025, 8, 11)),
    ("currentWeekStart", datetime(2025, 1, 1, 12), datetime(2024, 12, 30)),
    ("currentMonthStart", datetime(2025, 8, 14, 13, 47, 25, 123456), datetime(2025, 8, 1)),
    ("currentMonthStart", datetime(2024, 2, 29, 23, 59), datetime(2024, 2, 1)),
    ("currentQuarterStart", datetime(2025, 8, 14, 13, 47, 25, 123456), datetime(2025, 7, 1)),
    ("currentQuarterStart", datetime(2025, 3, 31, 23, 59), datetime(2025, 1, 1)),
    ("currentQuarterStart", datetime(2025, 4, 1, 0, 0), datetime(2025, 4, 1)),
    ("currentQuarterStart", datetime(2025, 12, 31, 23, 59), datetime(2025, 10, 1)),
    ("currentYearStart", datetime(2025, 8, 14, 13, 47, 25, 123456), datetime(2025, 1, 1)),
])
def test_reference_times(reference, date_time, expected):
    assert reference_times[reference](date_time) == expected


def test_reference_times_keep_timezone_of_aware_input():
    date_time = BRUSSELS.localize(datetime(2025, 8, 14, 13, 47, 25))

    result = reference_times["currentHourStart"](date_time)

    assert result == BRUSSELS.localize(datetime(2025, 8, 14, 13))
    assert result.utcoffset() == timedelta(hours=2)


@pytest.mark.xfail(strict=True, reason="reference times use replace() on pytz aware datetimes, so the day/week start "
                                       "keeps the UTC offset of the input instead of the offset valid at that start")
@pytest.mark.parametrize("reference, date_time, expected", [
    ("currentDayStart", datetime(2025, 3, 30, 12), datetime(2025, 3, 30)),
    ("currentDayStart", datetime(2025, 10, 26, 12), datetime(2025, 10, 26)),
    ("currentWeekStart", datetime(2025, 3, 30, 12), datetime(2025, 3, 24)),
])
def test_reference_times_on_dst_days_return_the_local_start_instant(reference, date_time, expected):
    result = reference_times[reference](BRUSSELS.localize(date_time))

    assert result == BRUSSELS.localize(expected)


def test_reference_times_default_to_now(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 8, 14, 13, 47, 25)

    monkeypatch.setattr(time_helper, "datetime", FixedDatetime)

    assert reference_times["currentHourStart"]() == datetime(2025, 8, 14, 13)
    assert reference_times["currentWeekStart"]() == datetime(2025, 8, 11)


@pytest.mark.parametrize("duration, expected", [
    ("PT1M", timedelta(minutes=1)),
    ("PT15M", timedelta(minutes=15)),
    ("PT1H30M", timedelta(hours=1, minutes=30)),
    ("P1D", timedelta(days=1)),
    ("P0D", timedelta(0)),
    ("P1W", timedelta(weeks=1)),
    ("P1DT5H", timedelta(days=1, hours=5)),
    ("-P1D", timedelta(days=-1)),
    ("-PT15M", timedelta(minutes=-15)),
    ("-P1DT5H", -timedelta(days=1, hours=5)),
])
def test_parse_duration(duration, expected):
    assert parse_duration(duration) == expected


@pytest.mark.parametrize("local_time, expected_utc", [
    (datetime(2025, 3, 30, 1, 30), datetime(2025, 3, 30, 0, 30)),  # before the spring change, CET
    (datetime(2025, 3, 30, 3, 30), datetime(2025, 3, 30, 1, 30)),  # after the spring change, CEST
    (datetime(2025, 10, 26, 1, 30), datetime(2025, 10, 25, 23, 30)),  # before the autumn change, CEST
    (datetime(2025, 10, 26, 3, 30), datetime(2025, 10, 26, 2, 30)),  # after the autumn change, CET
    (datetime(2025, 1, 15, 12, 0), datetime(2025, 1, 15, 11, 0)),
    (datetime(2025, 7, 15, 12, 0), datetime(2025, 7, 15, 10, 0)),
])
def test_convert_to_utc_assumes_brussels_for_naive_times(local_time, expected_utc):
    result = convert_to_utc(local_time)

    assert result == UTC.localize(expected_utc)
    assert result.utcoffset() == timedelta(0)


def test_convert_to_utc_uses_given_default_timezone():
    assert convert_to_utc(datetime(2025, 7, 15, 12), default_timezone="Europe/Vilnius") == UTC.localize(datetime(2025, 7, 15, 9))


def test_convert_to_utc_converts_aware_times_ignoring_default_timezone():
    aware = timezone("Europe/Vilnius").localize(datetime(2025, 7, 15, 12))

    result = convert_to_utc(aware, default_timezone="Europe/Brussels")

    assert result == UTC.localize(datetime(2025, 7, 15, 9))
    assert result.utcoffset() == timedelta(0)


@pytest.mark.parametrize("utc_time, expected_local, expected_offset_hours", [
    (datetime(2025, 3, 30, 0, 30), datetime(2025, 3, 30, 1, 30), 1),
    (datetime(2025, 3, 30, 1, 30), datetime(2025, 3, 30, 3, 30), 2),
    (datetime(2025, 10, 26, 0, 30), datetime(2025, 10, 26, 2, 30), 2),
    (datetime(2025, 10, 26, 1, 30), datetime(2025, 10, 26, 2, 30), 1),  # same wall clock, one hour later
])
def test_convert_to_timezone_assumes_utc_for_naive_times(utc_time, expected_local, expected_offset_hours):
    result = convert_to_timezone(utc_time)

    assert result.replace(tzinfo=None) == expected_local
    assert result.utcoffset() == timedelta(hours=expected_offset_hours)


def test_convert_to_timezone_uses_from_timezone_only_for_naive_times():
    naive = datetime(2025, 7, 15, 12)
    aware = BRUSSELS.localize(naive)

    assert convert_to_timezone(naive, from_timezone="Europe/Vilnius", to_timezone="UTC") == UTC.localize(datetime(2025, 7, 15, 9))
    assert convert_to_timezone(aware, from_timezone="Europe/Vilnius", to_timezone="UTC") == UTC.localize(datetime(2025, 7, 15, 10))


def test_convert_to_timezone_round_trips_convert_to_utc():
    local = datetime(2025, 10, 26, 3, 30)

    assert convert_to_timezone(convert_to_utc(local)).replace(tzinfo=None) == local


@pytest.mark.parametrize("iso_string, expected", [
    ("2025-06-10T12:00", UTC.localize(datetime(2025, 6, 10, 12))),
    ("2025-06-10T12:00Z", UTC.localize(datetime(2025, 6, 10, 12))),
    ("2025-06-10T12:00+02:00", UTC.localize(datetime(2025, 6, 10, 10))),
])
def test_parse_datetime_keeps_timezone_and_assumes_utc_when_missing(iso_string, expected):
    result = parse_datetime(iso_string)

    assert result.tzinfo is not None
    assert result == expected


def test_parse_datetime_keeps_given_offset():
    assert parse_datetime("2025-06-10T12:00+02:00").utcoffset() == timedelta(hours=2)


@pytest.mark.parametrize("iso_string", ["2025-06-10T12:00", "2025-06-10T12:00Z", "2025-06-10T12:00+02:00"])
def test_parse_datetime_without_timezone_drops_it_keeping_the_wall_clock(iso_string):
    assert parse_datetime(iso_string, keep_timezone=False) == datetime(2025, 6, 10, 12)
