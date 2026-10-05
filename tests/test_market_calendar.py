"""The collector skips market holidays and honours early closes.

Before 2026-09-29 it skipped weekends only; Labor Day 2026-09-07 stored 49
snapshots of the previous Friday's data.

Usage:  python3 tests/test_market_calendar.py
If `exchange_calendars` is importable, the hard-coded table is also re-checked
against its XNYS calendar (the source it was generated from); otherwise that
check is skipped, not failed.
"""
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from gex.collector import closed_reason, in_session, seconds_to_next_open
from gex.market_calendar import COVERED_THROUGH, EARLY_CLOSES, HOLIDAYS


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def main():
    ok = True
    print("holidays")
    # 2026-11-26 is in EST (UTC-5): 14:00 UTC = 09:00 ET, 16:00 UTC = 11:00 ET.
    ok &= check("Thanksgiving mid-morning: closed",
                closed_reason(utc(2026, 11, 26, 16)) == "market holiday: Thanksgiving")
    ok &= check("day before Thanksgiving mid-morning: open", in_session(utc(2026, 11, 25, 16)))
    # EDT (UTC-4): 15:00 UTC = 11:00 ET.
    ok &= check("Labor Day 2026 (the day that stored stale data): closed",
                not in_session(utc(2026, 9, 7, 15)), closed_reason(utc(2026, 9, 7, 15)))
    ok &= check("Good Friday 2027: closed", not in_session(utc(2027, 3, 26, 15)))
    ok &= check("ordinary Tuesday: open", in_session(utc(2026, 9, 29, 15)))
    ok &= check("weekend still closed", closed_reason(utc(2026, 10, 3, 15)) == "weekend")

    print("early close (2026-11-27, EST): SPY options 13:15 ET + 45 min tail = 14:00 ET")
    ok &= check("13:30 ET: still in the tail", in_session(utc(2026, 11, 27, 18, 30)))
    ok &= check("13:59 ET: still in the tail", in_session(utc(2026, 11, 27, 18, 59)))
    ok &= check("14:30 ET: closed, labelled early close",
                closed_reason(utc(2026, 11, 27, 19, 30)) == "after close (early close)")
    ok &= check("same clock time on a normal day: open", in_session(utc(2026, 11, 25, 19, 30)))
    ok &= check("normal day ends at 17:00 ET", not in_session(utc(2026, 11, 25, 22, 1)))

    print("next open skips holidays")
    # Wed 2026-11-25 17:00 ET -> Thu is Thanksgiving -> Fri 09:30 ET (14:30 UTC).
    s = seconds_to_next_open(utc(2026, 11, 25, 22))
    ok &= check("over Thanksgiving: Friday 09:30 ET", s == (utc(2026, 11, 27, 14, 30) - utc(2026, 11, 25, 22)).total_seconds(),
                f"{s/3600:.1f} h")
    # Thu 2026-07-02 18:00 ET (EDT) -> Fri Jul 3 holiday -> Mon Jul 6 09:30 ET.
    s = seconds_to_next_open(utc(2026, 7, 2, 22))
    ok &= check("over July 4th weekend: Monday 09:30 ET", s == (utc(2026, 7, 6, 13, 30) - utc(2026, 7, 2, 22)).total_seconds(),
                f"{s/3600:.1f} h")
    # DST: both operands must be compared in real elapsed time, not wall clock.
    # Fri 2026-03-06 17:00 EST -> clocks spring forward Sun 03-08 -> Mon 09:30 EDT.
    # Expected values are computed from the UTC instants, not typed in: a
    # hand-typed 65.5 h (from a review note) was itself wrong -- the true
    # fall-back gap is 64.5 h, and wall-clock arithmetic gives 63.5 h.
    s = seconds_to_next_open(utc(2026, 3, 6, 22))
    want = (utc(2026, 3, 9, 13, 30) - utc(2026, 3, 6, 22)).total_seconds()   # Mon 09:30 EDT
    ok &= check("across spring-forward: true elapsed time", s == want,
                f"{s/3600:.1f} h (want {want/3600:.1f}; wall clock would say 64.5)")
    # Fri 2026-10-30 18:00 EDT -> clocks fall back Sun 11-01 -> Mon 09:30 EST.
    s = seconds_to_next_open(utc(2026, 10, 30, 22))
    want = (utc(2026, 11, 2, 14, 30) - utc(2026, 10, 30, 22)).total_seconds()  # Mon 09:30 EST
    ok &= check("across fall-back: true elapsed time", s == want,
                f"{s/3600:.1f} h (want {want/3600:.1f}; wall clock would say 63.5)")
    s = seconds_to_next_open(utc(2026, 9, 29, 22))
    ok &= check("ordinary evening: next morning", s == (utc(2026, 9, 30, 13, 30) - utc(2026, 9, 29, 22)).total_seconds())

    print("table sanity")
    ok &= check("every holiday is a weekday", all(d.weekday() < 5 for d in HOLIDAYS))
    ok &= check("every early close is a weekday and not a holiday",
                all(d.weekday() < 5 and d not in HOLIDAYS for d in EARLY_CLOSES))
    ok &= check("table covers through 2028", COVERED_THROUGH >= date(2028, 12, 31))

    try:
        import exchange_calendars as xc
        import pandas as pd
    except ImportError:
        print("  SKIP  exchange_calendars cross-check (not installed)")
    else:
        c = xc.get_calendar("XNYS", start="2025-06-01", end="2028-12-29")
        sessions = set(d.date() for d in c.sessions_in_range("2026-01-01", "2028-12-29"))
        weekdays = [d.date() for d in pd.bdate_range("2026-01-01", "2028-12-29")]
        ref_hol = {d for d in weekdays if d not in sessions}
        ref_early = {d for d in sessions
                     if c.session_close(pd.Timestamp(d)).tz_convert("America/New_York").hour < 16}
        ok &= check("holidays match exchange_calendars XNYS", ref_hol == set(HOLIDAYS),
                    f"missing={sorted(ref_hol - set(HOLIDAYS))} extra={sorted(set(HOLIDAYS) - ref_hol)}")
        ok &= check("early closes match exchange_calendars XNYS", ref_early == set(EARLY_CLOSES),
                    f"missing={sorted(ref_early - set(EARLY_CLOSES))} extra={sorted(set(EARLY_CLOSES) - ref_early)}")

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
