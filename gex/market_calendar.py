"""US options-market holidays and early closes, hard-coded.

WHY: the collector originally skipped weekends only. On Labor Day 2026-09-07
it polled all day and stored 49 snapshots of the previous Friday's data --
stale data that reads back as a real session. Adding a dependency to the VM
for ~30 dates was not worth it, so the dates are written out here.

SOURCE: `exchange_calendars` 4.13.2, calendar XNYS, generated 2026-09-29 on
the Mac (not a project dependency). SPY options follow the NYSE/Cboe holiday
schedule. NYSE's published calendar already covers 2028; the table was
checked against it (2026-09-29), including that 2027-12-31 stays open.

NOT COVERED, by design: unscheduled closures (e.g. a national day of
mourning). Those still produce a stale-data day; exclude them by hand.

EARLY CLOSES: the NYSE closes at 13:00 ET. SPY options, which normally trade
to 16:15 ET, close at 13:15 ET on those days. The collector's 45-minute tail
is added on top of whichever close applies.
"""
from __future__ import annotations

from datetime import date, time as dtime

HOLIDAYS: dict[date, str] = {
    date(2026, 1, 1): "New Year's Day",
    date(2026, 1, 19): "Martin Luther King Jr. Day",
    date(2026, 2, 16): "Presidents' Day",
    date(2026, 4, 3): "Good Friday",
    date(2026, 5, 25): "Memorial Day",
    date(2026, 6, 19): "Juneteenth",
    date(2026, 7, 3): "Independence Day (observed)",
    date(2026, 9, 7): "Labor Day",
    date(2026, 11, 26): "Thanksgiving",
    date(2026, 12, 25): "Christmas",
    date(2027, 1, 1): "New Year's Day",
    date(2027, 1, 18): "Martin Luther King Jr. Day",
    date(2027, 2, 15): "Presidents' Day",
    date(2027, 3, 26): "Good Friday",
    date(2027, 5, 31): "Memorial Day",
    date(2027, 6, 18): "Juneteenth (observed)",
    date(2027, 7, 5): "Independence Day (observed)",
    date(2027, 9, 6): "Labor Day",
    date(2027, 11, 25): "Thanksgiving",
    date(2027, 12, 24): "Christmas (observed)",
    # 2028: New Year's Day falls on a Saturday and is not observed.
    date(2028, 1, 17): "Martin Luther King Jr. Day",
    date(2028, 2, 21): "Presidents' Day",
    date(2028, 4, 14): "Good Friday",
    date(2028, 5, 29): "Memorial Day",
    date(2028, 6, 19): "Juneteenth",
    date(2028, 7, 4): "Independence Day",
    date(2028, 9, 4): "Labor Day",
    date(2028, 11, 23): "Thanksgiving",
    date(2028, 12, 25): "Christmas",
}

# SPY options close (ET) on NYSE early-close days.
EARLY_CLOSES: dict[date, dtime] = {
    date(2026, 11, 27): dtime(13, 15),
    date(2026, 12, 24): dtime(13, 15),
    date(2027, 11, 26): dtime(13, 15),
    date(2028, 7, 3): dtime(13, 15),
    date(2028, 11, 24): dtime(13, 15),
}

COVERED_THROUGH = date(2028, 12, 31)


def holiday_name(d: date) -> str | None:
    """The holiday's name if the options market is closed that weekday."""
    return HOLIDAYS.get(d)


def options_close(d: date, regular: dtime) -> dtime:
    """The options close (ET) for `d`: the early close if one applies.
    NOTE: the early-close times are SPY's (13:15). For an S&P index option's
    expiry use settlement_time() -- expiring SPXW stops at 13:00."""
    return EARLY_CLOSES.get(d, regular)


# S&P 500 index options settlement clocks (ET), per Cboe's SPX specifications
# (cited in the design review, 2026-10-03): SPX (AM) settles on the
# Special Opening Quotation, i.e. from the open of its expiration date; SPXW
# (PM) at the close -- 16:00, or 13:00 on an early-close day (NOT SPY's
# 13:15 in EARLY_CLOSES). The listed OCC date already carries any holiday
# shift, so `expiry` is used as given.
AM_SETTLED_ROOTS = frozenset({"SPX"})
PM_SETTLED_INDEX_ROOTS = frozenset({"SPXW"})
INDEX_OPEN = dtime(9, 30)
INDEX_PM_CLOSE = dtime(16, 0)
INDEX_PM_EARLY_CLOSE = dtime(13, 0)


def settlement_time(root: str | None, expiry: date, regular: dtime) -> dtime:
    """When a contract of `root` expiring on `expiry` stops being a live
    position (ET): the AM settlement at the open, the PM index close, or --
    for any other root (SPY, roots we do not know) -- options_close()."""
    if root in AM_SETTLED_ROOTS:
        return INDEX_OPEN
    if root in PM_SETTLED_INDEX_ROOTS:
        return INDEX_PM_EARLY_CLOSE if expiry in EARLY_CLOSES else INDEX_PM_CLOSE
    return options_close(expiry, regular)


def covered(d: date) -> bool:
    """False once the table has run out -- the caller should warn loudly,
    because holidays past this date are no longer skipped."""
    return d <= COVERED_THROUGH
