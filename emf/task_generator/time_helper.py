# -------------------------------------------------------------------------------
# Name:        time_helper.py
# Purpose:     Collection of functions used for time manipulation
#
# Author:      kristjan.vilgo
#
# Created:     2023-04-11
# Copyright:   (c) Baltic RCC OÜ
# Licence:     MPL 2.0
# -------------------------------------------------------------------------------
from functools import wraps
from datetime import datetime, timedelta

import aniso8601
from aniso8601 import parse_duration as duration_parser
from pytz import timezone


def with_default_datetime_now(func):
    @wraps(func)
    def wrapper(date_time=None, *args, **kwargs):
        """
        A decorator that adds default datetime argument to a function.

        Args:
            date_time (datetime.datetime, optional): The datetime to use. If None,
                the current datetime is used.

        Returns:
            The result of calling the wrapped function with the modified datetime
            argument.

        """
        if not date_time:
            date_time = datetime.now()
        return func(date_time, *args, **kwargs)
    return wrapper


@with_default_datetime_now
def get_year_start(date_time):
    """
    Returns the datetime object corresponding to the start of the current year
    (January 1, 00:00:00).

    Args:
        date_time (datetime.datetime, optional): The datetime to use. If None,
            the current datetime is used.

    Returns:
        datetime.datetime: The datetime object corresponding to the start of the
        current year.

    """
    return date_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


@with_default_datetime_now
def get_month_start(date_time):
    """
    Returns the datetime object corresponding to the start of the current month
    (the first day of the month at 00:00:00).

    Args:
        date_time (datetime.datetime, optional): The datetime to use. If None,
            the current datetime is used.

    Returns:
        datetime.datetime: The datetime object corresponding to the start of the
        current month.

    """
    return date_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


@with_default_datetime_now
def get_week_start(date_time):
    """
    Returns the datetime object corresponding to the start of the current week
    (the first day of the week at 00:00:00).

    Note that the first day of the week is determined by the system locale.

    Args:
        date_time (datetime.datetime, optional): The datetime to use. If None,
            the current datetime is used.

    Returns:
        datetime.datetime: The datetime object corresponding to the start of the
        current week.

    """
    weekday = date_time.weekday()
    day_start = get_day_start(date_time)
    return day_start - timedelta(days=weekday)


@with_default_datetime_now
def get_day_start(date_time):
    """
    Returns the datetime object corresponding to the start of the current day
    (00:00:00).

    Args:
        date_time (datetime.datetime, optional): The datetime to use. If None,
            the current datetime is used.

    Returns:
        datetime.datetime: The datetime object corresponding to the start of the
        current day.

    """
    return date_time.replace(hour=0, minute=0, second=0, microsecond=0)


@with_default_datetime_now
def get_hour_start(date_time):
    """
    Returns the datetime object corresponding to the start of the current hour
    (the first minute of the hour at 00 seconds and 00 microseconds).

    Args:
        date_time (datetime.datetime, optional): The datetime to use. If None,
            the current datetime is used.

    Returns:
        datetime.datetime: The datetime object corresponding to the start of the
        current hour.

    """
    return date_time.replace(minute=0, second=0, microsecond=0)


@with_default_datetime_now
def get_quarter_start(date_time):
    """
    Returns the datetime object corresponding to the start of the current quarter.

    The quarters are defined as follows:
    - Q1: January 1 - March 31
    - Q2: April 1 - June 30
    - Q3: July 1 - September 30
    - Q4: October 1 - December 31

    Args:
        date_time (datetime.datetime, optional): The datetime to use. If None,
            the current datetime is used.

    Returns:
        datetime.datetime: The datetime object corresponding to the start of the
        current quarter.

    """
    quarter_start_month = 3 * ((date_time.month - 1) // 3) + 1
    return date_time.replace(month=quarter_start_month, day=1, hour=0, minute=0, second=0, microsecond=0)


@with_default_datetime_now
def get_minute_start(date_time):
    """
    Returns the datetime object corresponding to the start of the minute.

    If no datetime is provided, the current datetime is used.

    Args:
        date_time (datetime.datetime, optional): The datetime to use. If None,
            the current datetime is used.

    Returns:
        datetime.datetime: The datetime object corresponding to the start of the
        minute.
    """
    return date_time.replace(second=0, microsecond=0)


reference_times = {
    "currentMinuteStart": get_minute_start,
    "currentHourStart": get_hour_start,
    "currentDayStart": get_day_start,
    "currentWeekStart": get_week_start,
    "currentMonthStart": get_month_start,
    "currentQuarterStart": get_quarter_start,
    "currentYearStart": get_year_start
}


def parse_duration(iso8601_duration_string):
    """
    Parses an ISO 8601 duration string and returns the corresponding timedelta object.

    The duration string should be in the format 'PnYnMnDTnHnMnS', where:
    - 'P' is a mandatory prefix indicating the start of the duration
    - 'nY' indicates the number of years in the duration
    - 'nM' indicates the number of months in the duration
    - 'nD' indicates the number of days in the duration
    - 'T' is an optional separator indicating the start of the time portion of the duration
    - 'nH' indicates the number of hours in the duration
    - 'nM' indicates the number of minutes in the duration
    - 'nS' indicates the number of seconds in the duration

    This function allows parsing both positive and negative durations. If the input string
    starts with a '-' sign, the resulting timedelta object will be negated.

    Args:
        iso8601_duration_string (str): The ISO 8601 duration string to parse.

    Returns:
        datetime.timedelta: The timedelta object corresponding to the parsed duration.

    """
    if iso8601_duration_string[0] == "-":
        return duration_parser(iso8601_duration_string[1:]) * -1
    else:
        return duration_parser(iso8601_duration_string)


def convert_to_utc(date_time, default_timezone='Europe/Brussels'):
    """
    Converts a datetime object to UTC timezone.

    If the input datetime object does not have a timezone specified, it is assumed to be in the
    Europe/Brussels timezone or the timezone specified by the default_timezone parameter.

    Args:
        date_time (datetime.datetime): The datetime object to convert to UTC.
        default_timezone (str): The timezone to assume if the datetime object does not have one.
                                Default is 'Europe/Brussels'.

    Returns:
        datetime.datetime: The datetime object converted to UTC timezone.
    """
    if date_time.tzinfo is None:
        # Assume the datetime is in the default timezone if not defined
        date_time = timezone(default_timezone).localize(date_time)

    return date_time.astimezone(timezone("UTC"))

def parse_datetime(iso_string, keep_timezone=True):

    if keep_timezone:
        return aniso8601.parse_datetime(iso_string)
    else:
        return aniso8601.parse_datetime(iso_string).replace(tzinfo=None)