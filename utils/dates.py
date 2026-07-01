"""Shared date-range helpers for the report scripts.

Every monthly report targets the previous full calendar month, and every weekly
report targets the previous full Monday–Sunday week. This module centralises that
arithmetic, which was previously re-derived in each script in a few different ways
(including fragile string slicing of ISO dates).
"""
import calendar
from datetime import datetime, timedelta, timezone

DATE_FORMAT = "%Y-%m-%d"


def previous_month(today=None):
  """Return values describing the previous full calendar month.

  Returns a 4-tuple of strings/int matching what the report scripts expect:
    start_date    -> first day, "%Y-%m-%d"  (e.g. "2026-06-01")
    end_date      -> last day,  "%Y-%m-%d"  (e.g. "2026-06-30")
    days_in_month -> int day count of that month (e.g. 30)
    output_date   -> "%b_%Y"                (e.g. "Jun_2026")
  """
  today = today or datetime.today()
  last_month_date = today.replace(day=1) - timedelta(days=1)
  year, month = last_month_date.year, last_month_date.month
  days_in_month = calendar.monthrange(year, month)[1]
  start = datetime(year, month, 1)
  end = datetime(year, month, days_in_month)
  return (
    start.strftime(DATE_FORMAT),
    end.strftime(DATE_FORMAT),
    days_in_month,
    start.strftime("%b_%Y"),
  )


def previous_week_start(utc_offset_hours, now=None):
  """Return values describing the Monday of the previous full week.

  Local time is UTC shifted by a fixed integer hour offset, matching the
  existing weekly scripts (HK=+8, TH/VN=+7).

  Returns a 2-tuple of strings:
    start_date  -> previous week's Monday, "%Y-%m-%d"
    output_date -> "%d_%b_%Y"
  """
  local_now = (now or datetime.now(timezone.utc)) + timedelta(hours=utc_offset_hours)
  start_date = (local_now - timedelta(days=local_now.weekday() + 7)).strftime(DATE_FORMAT)
  output_date = datetime.strptime(start_date, DATE_FORMAT).strftime("%d_%b_%Y")
  return start_date, output_date
