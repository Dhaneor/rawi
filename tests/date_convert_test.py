#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Nov 20 20:56:00 2024

@author dhaneor
"""
import os
import pytest
import sys
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------------------------------
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# ------------------------------------------------------------------------------------
from util.date_convert import date_convert, interval_to_milliseconds  # noqa: E402


def unix_to_datetime(unix_timestamp):
    """Convert Unix timestamp (milliseconds) to a datetime object."""
    return datetime.fromtimestamp(unix_timestamp / 1000, tz=timezone.utc)


# Test function to be decorated
@date_convert()
def sample_function(regular_param, start=None, end=None, interval=None):
    return regular_param, start, end, interval


# Test cases
@pytest.mark.parametrize(
    "start_date, end_date, expected_start, expected_end",
    [
        ("2023-11-20", "2023-11-25", 1700438400000, 1700870400000),
        ("20 Nov 2023", "25 November 2023", 1700438400000, 1700870400000),
        (
            "November 20, 2023 00:00:00",
            "November 25, 2023",
            1700438400000,
            1700870400000,
        ),
        ("20/11/2023", "25/11/2023", 1700438400000, 1700870400000),
        ("11/20/2023", "11/25/2023", 1700438400000, 1700870400000),
        ("2023.11.20", "2023.11.25", 1700438400000, 1700870400000),
    ],
)
def test_date_convert_various_formats(
    start_date, end_date, expected_start, expected_end
):
    _, start, end, _ = sample_function(
        "test", start=start_date, end=end_date, interval="1d"
    )

    # Convert timestamps to human-readable format for debugging
    start_readable = unix_to_datetime(start).strftime("%Y-%m-%d %H:%M:%S")
    end_readable = unix_to_datetime(end).strftime("%Y-%m-%d %H:%M:%S")
    expected_start_readable = unix_to_datetime(expected_start).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expected_end_readable = unix_to_datetime(expected_end).strftime("%Y-%m-%d %H:%M:%S")

    assert start == expected_start, (
        f"Start date start date should be: {expected_start} ({expected_start_readable})"
        f", but got: {start} ({start_readable})"
    )
    assert end == expected_end, (
        f"End date mismatch for input '{end_date}':\n"
        f"Expected: {expected_end} ({expected_end_readable})\n"
        f"Got: {end} ({end_readable})"
    )


@pytest.mark.parametrize(
    "start_date, end_date, expected_start_delta, expected_end_delta",
    [
        ("today UTC", "tomorrow UTC", timedelta(days=0), timedelta(days=1)),
        ("now UTC", "in 1 hour UTC", timedelta(seconds=0), timedelta(hours=1)),
        ("1 hour ago UTC", "now UTC", timedelta(hours=-1), timedelta(seconds=0)),
        (
            "100 days ago UTC",
            "today UTC",
            timedelta(days=-100, hours=1),
            timedelta(days=0),
        ),
        (
            "1 month ago UTC",
            "now UTC",
            relativedelta(months=-1, hours=1),
            timedelta(seconds=0),
        ),
        ("last week UTC", "next week UTC", timedelta(weeks=-1), timedelta(weeks=1)),
    ],
)
def test_date_convert_relative_dates(
    start_date, end_date, expected_start_delta, expected_end_delta
):
    _, start, end, interval = sample_function(
        "test", start=start_date, end=end_date, interval="1d"
    )
    now = datetime.now().replace(microsecond=0)

    expected_start = int((now + expected_start_delta).timestamp() * 1000)
    expected_end = int((now + expected_end_delta).timestamp() * 1000)

    # Convert Unix timestamps to datetime objects
    start_datetime = unix_to_datetime(start)
    end_datetime = unix_to_datetime(end)
    expected_start_datetime = unix_to_datetime(expected_start)
    expected_end_datetime = unix_to_datetime(expected_end)

    # Format datetimes as strings for easier comparison and debugging
    start_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    expected_start_str = expected_start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    expected_end_str = expected_end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # Allow for a small time difference (e.g., 2 seconds) due to execution time
    tolerance = timedelta(seconds=2)

    assert (
        abs(start_datetime - expected_start_datetime) <= tolerance
    ), f"Start date should be close to {expected_start_str}, but got {start_str}"
    assert (
        abs(end_datetime - expected_end_datetime) <= tolerance
    ), f"End date should be close to {expected_end_str}, but got {end_str}"

    # Print the dates for debugging
    print(f"\nInput: start='{start_date}', end='{end_date}'")
    print(f"Converted: start={start_str}, end={end_str}")
    print(f"Expected:  start={expected_start_str}, end={expected_end_str}")


def test_date_convert_invalid_date():
    with pytest.raises(
        Exception
    ):  # You might want to use a more specific exception here
        sample_function("test", start="invalid date")


def test_date_convert_no_date_params():
    result, start, end, interval = sample_function("test", interval="1d")

    now = int(datetime.now(timezone.utc).replace(microsecond=0).timestamp()) * 1000

    assert result == "test"
    assert start == now - interval_to_milliseconds(interval) * 1000
    assert end == now
    assert interval == "1d"


@date_convert()
def function_with_additional_params(
    regular_param, other_param, start=None, end=None, interval="1d"
):
    return regular_param, other_param, start, end, interval


def test_date_convert_with_additional_params():
    reg, other, start, end, interval = function_with_additional_params(
        "reg", "other", start="2023-11-20", end="2023-11-25", interval="1d"
    )
    assert reg == "reg"
    assert other == "other"
    assert start == 1700438400000
    assert end == 1700870400000
    assert interval == "1d"


def test_date_convert_only_start():
    _, start, end, interval = sample_function("test", start="2023-11-20", interval="1d")
    assert start == 1700438400000
    assert end == int(datetime.now(timezone.utc).timestamp()) * 1000


def test_date_convert_only_end():
    interval = "1d"
    _, start, end, interval = sample_function(
        "test", end="2023-11-25", interval=interval
    )

    # make sure that start is -1000 intervals before end
    assert start == 1700870400000 - interval_to_milliseconds(interval) * 1000
    assert end == 1700870400000


if __name__ == "__main__":
    pytest.main([__file__])
