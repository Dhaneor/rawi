#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a decorator function that converts datetimes to unix timestamps.

The decorator is intended to be used on functions that accept start and
end dates as input parameters. The decorator will convert the datetimes to
unix timestamps before calling the decorated function.

The datetimes can have the following formats:
- YYYY-MM-DD
- YYYY-MM-DD HH:MM:SS
- YYYY-MM-DD HH:MM:SS.sss
- "Month DD, YYYY HH:MM:SS"
- "Month DD, YYYY HH:MM:SS AM/PM"
- "now UTC"
- "one hour ago"
- "three days ago"

See dateparser documentation for more details:
https://dateparser.readthedocs.io/en/latest/

The unix timestamps will be returned in milliseconds or seconds,
depending on the parameter that is set for the decorator (default
is 'ms').

Created on Nov 20 20:29:20 2024

@author dhaneor
"""
import dateparser
import re

from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Optional, Callable
from zoneinfo import ZoneInfo

EPOCH: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
MILLISECONDS_THRESHOLD = 1e10


class UnknownDateFormat(Exception):
    """Raised when the date string does not match any known format."""


def date_to_milliseconds(date_str: str) -> int:
    """Convert UTC date to milliseconds

    If using offset strings add "UTC" to date string,
    e.g. "now UTC", "11 hours ago UTC"

    See dateparse docs for formats:
    http://dateparser.readthedocs.io/en/latest/

    Arguments:
    ----------
    date_str: str
        date in readable format,
        i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"

    Returns:
    --------
    int
        the provided date in milliseconds since epoch
    """
    # get epoch value in UTC

    # parse our date string
    d: Optional[datetime] = dateparser.parse(date_str, settings={"TIMEZONE": "UTC"})
    if not d:
        raise UnknownDateFormat(date_str)

    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=ZoneInfo('UTC'))

    # return the difference in time
    return int((d - EPOCH).total_seconds() * 1000.0)


def interval_to_milliseconds(interval: str) -> int:
    pattern = r'(\d+)([smhdw])'
    match = re.match(pattern, interval)
    if match:
        value, unit = match.groups()
        seconds_per_unit = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}
        return int(value) * seconds_per_unit[unit] * 1000
    else:
        raise ValueError(f"Invalid interval format: '{interval}'")


def integer_to_timestamp(
    time_value,
    is_start=False,
    end_time=None,
    interval=None
) -> int:
    """
    Convert an integer timestamp to a datetime object or calculate a relative timestamp.

    This function handles both positive and negative timestamps, with special
    behavior for negative start times relative to an end time.

    Parameters:
    -----------
    time_value : int
        The timestamp to convert. Can be in milliseconds or seconds.
    is_start : bool, optional
        Flag indicating if this is a start time (default is False).
    end_time : int, optional
        The end timestamp, required if time_value is negative and is_start is True.
    interval : str, optional
        The interval string (e.g., '1m', '1h') used for calculating relative time.

    Returns:
    --------
    datetime
        A timezone-aware datetime object representing the converted timestamp.

    Raises:
    -------
    ValueError
        If start is negative and end_time is not provided,
        or if the interval is invalid.
    """
    # Check if the integer is likely to be in secs or ms
    time_value = time_value / 1000 \
        if abs(time_value) > MILLISECONDS_THRESHOLD else time_value

    if time_value < 0 and is_start:
        if end_time is None:
            raise ValueError("If start is negative, end time must be provided")

        # determine the length of one interval in ms
        if (interval_ms := interval_to_milliseconds(interval)) is None:
            raise ValueError(f"Invalid interval: {interval}")

        # Calculate time based on intervals before end time
        end_time = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)
        delta = timedelta(milliseconds=interval_ms * abs(time_value))
        return end_time - delta

    else:
        return datetime.fromtimestamp(time_value, tz=timezone.utc)


def parse_datestring(date_str: str) -> int:
    """
    Parse a date string and convert it to a UTC datetime object.

    This function uses the dateparser library to parse various date string formats
    and convert them to a UTC-aware datetime object.

    Parameters:
    -----------
    date_str : str
        The date string to be parsed. Can be in various formats supported by dateparser.

    Returns:
    --------
    datetime
        A UTC-aware datetime object representing the parsed date.

    Raises:
    -------
    ValueError
        If the date string cannot be parsed.

    """
    result_time = dateparser.parse(date_str, settings={"TIMEZONE": "UTC"})

    if not result_time:
        raise ValueError(f"Unable to parse date string: {date_str}")

    return result_time.replace(tzinfo=ZoneInfo('utc'))


def date_convert(unit: str = 'ms') -> Callable:
    """
    A decorator that converts datetime inputs to unix timestamps.

    This decorator is designed to be used on functions that accept
    'start' and 'end' datetime parameters. It converts these datetime
    inputs to unix timestamps before calling the decorated function.
    The decorator also handles various datetime formats and relative
    time expressions.

    Parameters:
    -----------
    unit : str, optional
        The unit of time for the returned timestamps. Default is
        'ms' for milliseconds. Use 's' for seconds.

    Returns:
    --------
    Callable
        A decorator function that wraps the input function and performs
        the datetime to timestamp conversion.

    Notes:
    ------
    The decorated function should expect 'start', 'end', and 'interval'
    as keyword arguments.

    If 'start' is not provided, it defaults to 1000 intervals before 'end'.
    The 'interval' parameter is required and must be provided as a keyword
    argument to the decorated function if start is None or negative.

    If 'end' is not provided, it defaults to the current time.

    Raises:
    -------
    ValueError
        If the 'interval' is not provided, if the end time is earlier
        than the start time,  or if an unsupported time value type is
        provided.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = kwargs.get("start")
            end = kwargs.get("end")
            interval = kwargs.get("interval")

            if start is None:
                start = -1000

            if end is None:
                end = int(datetime.now(timezone.utc).timestamp()) * 1000

            if not interval:
                raise ValueError("interval must be provided as a keyword argument")

            def process_time(time_value, is_start=False, end_time=None):
                if isinstance(time_value, int):
                    result_time = integer_to_timestamp(
                        time_value=time_value,
                        is_start=is_start,
                        end_time=end_time,
                        interval=interval
                        )
                elif isinstance(time_value, str):
                    result_time = parse_datestring(time_value)
                else:
                    raise ValueError(
                        f"Unsupported time value type: {type(time_value)}"
                        )

                return result_time.timestamp() * 1000 \
                    if unit == 'ms' else result_time.timestamp()

            # ........................................................................
            # process start and end time
            kwargs["end"] = process_time(end)
            kwargs["start"] = process_time(start, True, kwargs['end'])

            # Validate that end time is not earlier than start time
            if kwargs.get('end') < kwargs.get('start'):
                raise ValueError("end time cannot be earlier than start time")

            return func(*args, **kwargs)
        return wrapper
    return decorator
