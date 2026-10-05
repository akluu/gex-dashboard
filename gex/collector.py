"""Polling daemon. Fetch -> aggregate -> persist, forever.

Design constraints, all from the original design notes:

  * Poll only when it can matter. The source is delayed and OI updates DAILY,
    so hammering it overnight buys nothing and burns goodwill with a CDN whose
    terms already prohibit auto-extraction. Sessions are Eastern.
  * Never overwrite good state with bad. A failed or unparseable fetch is
    logged and skipped -- it must not land in the aggregate store, because a
    plausible-looking zero row is indistinguishable from a flat market later.
  * Back off visibly on failure and recover on its own. This runs unattended;
    the archived 5-snapshot history exists precisely because a collector died
    quietly and nobody noticed for days.
  * Skip unchanged payloads. The source is a CDN with its own refresh cadence;
    re-storing a byte-identical response inflates the archive without adding
    information.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

from .aggregate import aggregate, net_exposure, session_date, _NY


def seconds_to_next_open(now: datetime | None = None) -> float:
    """Seconds until the next session open, so idle sleeps never overshoot it.
    Skips weekends AND market holidays."""
    now = (now or datetime.now(timezone.utc)).astimezone(_NY)
    cand = now.replace(hour=RTH_OPEN.hour, minute=RTH_OPEN.minute,
                       second=0, microsecond=0)
    while cand <= now or cand.weekday() >= 5 or holiday_name(cand.date()):
        cand += timedelta(days=1)
        cand = cand.replace(hour=RTH_OPEN.hour, minute=RTH_OPEN.minute,
                            second=0, microsecond=0)
    # Subtract in UTC. Two datetimes sharing the _NY tzinfo subtract as WALL
    # CLOCK, so across a DST change this was off by an hour (reproduced:
    # 64.5 h vs a true 63.5 h over the March 2026 change). Harmless in
    # practice -- idle sleeps are capped at 15 min and recomputed -- but wrong.
    return (cand.astimezone(timezone.utc) - now.astimezone(timezone.utc)).total_seconds()
from .fetch import UNCHANGED, CboeClient
from . import store
from .market_calendar import COVERED_THROUGH, covered, holiday_name, options_close

# US equity-options RTH, Eastern. Collection continues past the close by
# SESSION_TAIL because the feed is delayed -- the last real prints arrive
# after the bell.
RTH_OPEN = dtime(9, 30)
RTH_CLOSE = dtime(16, 15)          # SPX options close 16:15 ET
SESSION_TAIL = timedelta(minutes=45)


def in_session(now: datetime | None = None) -> bool:
    return closed_reason(now) is None


def market_session(now: datetime | None = None) -> tuple[bool, str]:
    """Whether SPY options' REGULAR session is open (09:30 to the options
    close, early closes honoured) -- NOT the collector's window, which runs
    SESSION_TAIL longer. For display; the collector gates on in_session()."""
    now = (now or datetime.now(timezone.utc)).astimezone(_NY)
    if now.weekday() >= 5:
        return False, "weekend"
    name = holiday_name(now.date())
    if name:
        return False, f"market holiday: {name}"
    close = options_close(now.date(), RTH_CLOSE)
    start = now.replace(hour=RTH_OPEN.hour, minute=RTH_OPEN.minute, second=0, microsecond=0)
    end = now.replace(hour=close.hour, minute=close.minute, second=0, microsecond=0)
    if now < start:
        return False, "before the open"
    if now >= end:
        return False, "after the close" + (" (early close)" if close != RTH_CLOSE else "")
    return True, "regular session" + (" (early close day)" if close != RTH_CLOSE else "")


def closed_reason(now: datetime | None = None) -> str | None:
    """None during the session (open through close + SESSION_TAIL, early
    closes honoured); otherwise why not. Holidays matter: before 2026-09-29
    only weekends were skipped, and Labor Day 2026-09-07 stored a full day of
    the previous Friday's data."""
    now = (now or datetime.now(timezone.utc)).astimezone(_NY)
    if now.weekday() >= 5:
        return "weekend"
    name = holiday_name(now.date())
    if name:
        return f"market holiday: {name}"
    close = options_close(now.date(), RTH_CLOSE)
    start = now.replace(hour=RTH_OPEN.hour, minute=RTH_OPEN.minute, second=0, microsecond=0)
    end = now.replace(hour=close.hour, minute=close.minute, second=0,
                      microsecond=0) + SESSION_TAIL
    if now < start:
        return "before open"
    if now > end:
        return "after close" + (" (early close)" if close != RTH_CLOSE else "")
    return None


# Backoff on a failure streak, as a multiple of the base interval (2026-10-01
# change). In session, for a source that did NOT answer
# (timeout, connection error, an unparseable body, an exception in our own
# cycle) the cap is 4 x --interval: that morning a 16x cap left 16-minute holes
# at the open, and the source has no backfill. This is an engineering
# compromise, not a reading of Cboe's terms: at the cap an outage costs at most
# 15 requests an hour against the normal 60. When the source DID answer with
# an HTTP error (403, 429, 5xx...) it is telling us something, so the 16x cap
# stays, and a Retry-After header becomes an absolute deadline before which no
# request is made at all -- across session changes, and NOT shortened by the
# open: the source's explicit request outranks our backoff.
# Outside the session the 16x cap is unchanged and never sleeps past the open.
IN_SESSION_BACKOFF_CAP = 4
IDLE_BACKOFF_CAP = 16
RETRY_AFTER_MAX_S = 48 * 3600.0      # only a guard against an absurd header value
# Unusable payloads (the source answered, but the data fails validation --
# daily, CBOE's greeks-zeroed handover, ~6 min) are polled at the normal
# cadence for this long after the first one, then back off like a failure,
# whatever the cause: a lasting format break must not be polled every minute
# forever (a complete response proves transport, not source health).
UNUSABLE_GRACE_S = 900.0
# In session, one ALERT line when no usable result (stored, or unchanged since
# the last stored snapshot) has come for this long, and one "recovered" line.
STALL_ALERT_S = 600.0


def next_delay(active: bool, fail_streak: int, interval: float, idle_interval: float,
               to_open: float | None = None, http_refusal: bool = False) -> float:
    """Seconds of OUR backoff before the next poll (a Retry-After deadline is
    separate: Collector._retry_at). Outside the session it never sleeps past
    the open (`to_open`, seconds), so an overnight streak cannot swallow the
    first hours of a session."""
    base = interval if active else idle_interval
    cap = IN_SESSION_BACKOFF_CAP if active and not http_refusal else IDLE_BACKOFF_CAP
    delay = base * min(2 ** fail_streak, cap) if fail_streak else base
    if not active:
        delay = min(delay, max(5.0, to_open if to_open is not None else delay))
    return delay


def retry_after_seconds(value: str | None, now: datetime) -> float | None:
    """A Retry-After header (delta-seconds or an HTTP date) in seconds, else None."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        return max(0.0, (parsedate_to_datetime(value) - now).total_seconds())
    except (TypeError, ValueError):
        return None


class Collector:
    def __init__(self, symbol: str, root: Path, interval: float,
                 idle_interval: float, always: bool, raw_until: date | None = None) -> None:
        self.client = CboeClient(symbol)
        self.symbol = symbol
        self.root = Path(root)
        # Last session date (New York) of the raw-every-poll window, or None.
        self.raw_until = raw_until
        self._raw_window_closed_logged = False
        self.interval = interval
        self.idle_interval = idle_interval
        self.always = always
        self._stop = False
        self._fail_streak = 0      # failures driving the backoff (see next_delay)
        self._http_refusal = False # the last failure was an HTTP error response
        self._retry_at: float | None = None   # Retry-After deadline (epoch s): no request before it
        self._bad_streak = 0       # unusable payloads since the last usable result
        self._bad_since: float | None = None
        self._last_ok: float | None = None   # last usable result (stored or unchanged)
        self._stall_alerted = False
        self._stall_was_in = False # whether the last stall check was in session
        self._now = time.time      # the clock, replaceable in tests
        self._in_session = lambda: self.always or closed_reason() is None
        self._was_active = False
        self._calendar_warned: str | None = None
        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)

    def _handle_stop(self, *_) -> None:
        self.log("received stop signal, finishing current cycle")
        self._stop = True

    def log(self, msg: str, **fields) -> None:
        """One JSON object per line -- greppable. The dashboard's status panel
        reads the store and the validation reports, NOT this log, so a stall
        ALERT is visible here only."""
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "msg": msg, **fields}
        print(json.dumps(rec), flush=True)

    def _delay(self, active: bool, to_open: float | None = None) -> float:
        return next_delay(active, self._fail_streak, self.interval, self.idle_interval, to_open,
                          http_refusal=self._http_refusal)

    def _sleep(self, seconds: float) -> None:
        """Sleep, interruptibly, checking for a stall every 15 s -- an outage
        spent waiting (a long backoff or a Retry-After) must still raise the
        alert on time, not only after the next fetch."""
        for i in range(int(seconds)):
            if self._stop:
                return
            time.sleep(1)
            if i % 15 == 14:
                self._check_stall()

    def _fail(self, http_refusal: bool = False) -> None:
        self._fail_streak += 1
        self._http_refusal = http_refusal

    def _usable_ok(self) -> None:
        """A usable result: stored, or unchanged since the last stored one."""
        self._check_stall()        # an outage that crossed the threshold unseen is still reported
        now = self._now()
        if self._stall_alerted:
            self.log("recovered from stall", stalled_min=round((now - (self._last_ok or now)) / 60, 1))
            self._stall_alerted = False
        self._fail_streak = self._bad_streak = 0
        self._http_refusal, self._bad_since = False, None
        self._last_ok = now

    def _check_stall(self) -> None:
        """In session only: one ALERT line once STALL_ALERT_S pass without a usable
        result. The session transition is handled HERE, before any comparison,
        so the clock starts at the open whichever check sees it first -- a
        check inside _sleep() at the open must not report the night."""
        now = self._now()
        if not self._in_session():
            self._stall_was_in = False
            return
        if not self._stall_was_in or self._last_ok is None:   # a session (re)opened, or startup
            self._stall_was_in = True
            self._last_ok, self._stall_alerted = now, False
            return
        gap = now - self._last_ok
        if gap >= STALL_ALERT_S and not self._stall_alerted:
            self._stall_alerted = True
            self.log("ALERT: no usable snapshot in session", minutes=round(gap / 60, 1),
                     fail_streak=self._fail_streak, bad_payload_streak=self._bad_streak)

    def _checkpoint(self, snap) -> bool:
        """The daily raw checkpoint, kept apart from snapshot availability: a
        failure is logged as such and retried on every later usable poll --
        stored OR unchanged -- until today's file exists (daily_checkpoint is
        a no-op once it does). It never fails the cycle or feeds the backoff."""
        try:
            return bool(store.daily_checkpoint(self.root, snap))
        except Exception as e:
            self.log("daily checkpoint FAILED (snapshot itself is stored)", error=f"{type(e).__name__}: {e}")
            return False

    def _raw_poll(self, snap) -> None:
        """Raw-every-poll window (see store.save_raw_poll): keep the exact body
        of EVERY response before anything can reject it or raise. A failure
        here is logged and never fails the cycle or feeds the backoff."""
        try:
            if self.raw_until is None:
                return
            sess = session_date(snap.received_at)
            if sess > self.raw_until:
                if not self._raw_window_closed_logged:
                    self._raw_window_closed_logged = True
                    self.log("raw-every-poll window ended", raw_until=str(self.raw_until))
                return
            store.save_raw_poll(self.root, snap, sess)
        except Exception as e:
            self.log("raw poll save FAILED", error=f"{type(e).__name__}: {e}")

    def cycle(self) -> bool:
        """One poll. Returns True if a snapshot was stored.

        EVERY failure is contained here. An exception escaping into
        systemd's Restart=always would retry in 30s with a fresh failure
        streak, defeating the backoff and hammering a source whose terms
        already prohibit auto-extraction.
        """
        try:
            return self._cycle_inner()
        except Exception as e:
            self._fail()
            self.log("cycle failed", error=f"{type(e).__name__}: {e}",
                     fail_streak=self._fail_streak)
            return False

    def _cycle_inner(self) -> bool:
        try:
            snap = self.client.fetch()
        except Exception as e:
            self._fail()
            self.log("fetch failed", error=f"{type(e).__name__}: {e}",
                     fail_streak=self._fail_streak)
            return False

        self._raw_poll(snap)        # FIRST: before any rejection or exception below

        if snap.http_status != 200 or not snap.payload:
            refusal = snap.http_status != 200
            ra = retry_after_seconds(snap.retry_after, snap.received_at) if refusal else None
            self._fail(http_refusal=refusal)
            if ra:
                self._retry_at = self._now() + min(ra, RETRY_AFTER_MAX_S)
            msg = ("ACCESS DENIED by the source" if snap.http_status in (401, 403)
                   else "bad response")
            self.log(msg, status=snap.http_status, warnings=snap.warnings, retry_after_s=ra,
                     no_request_until=datetime.fromtimestamp(self._retry_at, timezone.utc).isoformat()
                     if ra else None, fail_streak=self._fail_streak)
            return False

        agg = aggregate(snap.payload)
        # Validate BEFORE the unchanged check: a repeated BAD payload was
        # being classified "unchanged" and resetting the streak, so a
        # persistently broken source looked healthy. An unusable payload is
        # counted on its OWN streak and is polled at the normal cadence for a
        # grace period (UNUSABLE_GRACE_S, 15 min) -- the CDN answered in full,
        # so that is the normal load -- and only then backs off like a failure.
        # (It
        # used to share the backoff streak, and CBOE's daily greeks-zeroed
        # handover -- three rejections on 2026-10-01 -- pre-loaded the backoff
        # so that the timeouts that followed waited 8 and then 16 minutes.)
        if not agg.get("usable"):
            now = self._now()
            if self._bad_since is None:
                self._bad_since = now
            self._bad_streak += 1
            grace = now - self._bad_since <= UNUSABLE_GRACE_S
            if grace:                      # the source answered: normal cadence for now
                self._fail_streak, self._http_refusal = 0, False
            else:                          # lasting bad data: back off like a failure
                self._fail()
            self.log("payload not usable, NOT stored", problems=agg.get("problems"),
                     bad_payload_streak=self._bad_streak, bad_for_s=round(now - self._bad_since),
                     backing_off=not grace, fail_streak=self._fail_streak)
            return False

        if UNCHANGED in snap.warnings:
            self._usable_ok()
            self.log("unchanged, not stored", fingerprint=snap.fingerprint,
                     snapshot_time=str(snap.snapshot_time))
            self._checkpoint(snap)         # retries a failed daily checkpoint; no-op once today's exists
            return False

        net = net_exposure(agg, per_pct=True)
        store.append_metadata(self.root, snap, agg, net)
        store.append_aggregate(self.root, snap, agg)
        self.client.mark_stored(snap.fingerprint)    # only now is an identical body "unchanged"
        ckpt = self._checkpoint(snap)

        self._usable_ok()
        self.log("stored",
                 spot=agg.get("spot"),
                 rows=agg.get("n_rows"),
                 contracts=agg.get("n_contracts"),
                 net_gex_bn=round((net["net_gex"] or 0) / 1e9, 2),
                 net_dex_bn=round((net["net_dex"] or 0) / 1e9, 2),
                 fetch_s=round(snap.fetch_seconds, 3),
                 snapshot_time=str(snap.snapshot_time),
                 newest_trade=snap.newest_trade_time,   # the honest staleness input
                 daily_checkpoint=bool(ckpt))
        return True

    def run(self) -> int:
        self.log("collector starting", symbol=self.symbol, root=str(self.root),
                 interval=self.interval, idle_interval=self.idle_interval,
                 always=self.always, session_date=str(session_date()),
                 raw_every_poll_until=str(self.raw_until) if self.raw_until else None)
        while not self._stop:
            today = datetime.now(timezone.utc).astimezone(_NY).date()
            if not covered(today) and self._calendar_warned != today.isoformat():
                # Once per day, loudly: past the table's end, holidays are
                # silently polled again (the pre-2026-09-29 behaviour).
                self._calendar_warned = today.isoformat()
                self.log("MARKET CALENDAR EXPIRED -- holidays are NOT being "
                         "skipped; extend gex/market_calendar.py",
                         covered_through=str(COVERED_THROUGH))
            reason = None if self.always else closed_reason()
            active = reason is None
            if active and not self._was_active:
                if self._fail_streak or self._bad_streak:
                    self.log("session opened, clearing stale failure streak",
                             previous_streak=self._fail_streak, previous_bad_streak=self._bad_streak)
                self._fail_streak = self._bad_streak = 0
                self._http_refusal, self._bad_since = False, None    # a Retry-After deadline is KEPT
            self._was_active = active
            # The source asked us to wait: no request before the deadline, but
            # wake at least once a minute for the session bookkeeping above
            # and the stall check (in _sleep).
            if self._retry_at is not None:
                left = self._retry_at - self._now()
                if left > 0:
                    self._sleep(max(1.0, min(left, 60.0)))
                    continue
                self._retry_at = None
            if active:
                self.cycle()
                self._check_stall()
            else:
                self.log("outside session, idling", reason=reason)
            # Exponential backoff on a failure streak, capped (next_delay), so
            # a source outage does not become a tight retry loop against a CDN
            # whose terms we are already near the edge of. Outside the session
            # it never sleeps past the open: an overnight streak (16 x 900 s =
            # 4 hours) could otherwise swallow the first hours of a session,
            # and even a clean 900 s idle could start the first poll ~15
            # minutes late.
            self._sleep(self._delay(active, None if active else seconds_to_next_open()))
        self.log("collector stopped cleanly")
        return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="CBOE options-chain collector")
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--root", default="data")
    ap.add_argument("--interval", type=float, default=60.0,
                    help="seconds between polls during the session")
    ap.add_argument("--idle-interval", type=float, default=900.0,
                    help="seconds between checks outside the session")
    ap.add_argument("--always", action="store_true",
                    help="poll regardless of session hours (for testing)")
    ap.add_argument("--once", action="store_true", help="single cycle, then exit")
    ap.add_argument("--raw-every-poll-until", type=date.fromisoformat, default=None,
                    metavar="YYYY-MM-DD",
                    help="keep the exact body of every response under raw_polls/ through "
                         "this New York session date (schema-transition insurance)")
    a = ap.parse_args(argv)

    c = Collector(a.symbol, Path(a.root), a.interval, a.idle_interval, a.always,
                  raw_until=a.raw_every_poll_until)
    if a.once:
        return 0 if c.cycle() else 1
    return c.run()


if __name__ == "__main__":
    sys.exit(main())
