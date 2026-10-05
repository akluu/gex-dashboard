"""CBOE delayed-quotes client.

Deliberately thin: one persistent session, compression, and an honest record
of WHEN things happened. The three clocks that matter are kept separate
(see the original design notes):

  1. market/event time   -- when a trade actually occurred (`last_trade_time`)
  2. snapshot time       -- the payload's own top-level `timestamp`
  3. receipt time        -- when WE got it

Conflating (2) with data age is the specific error this module exists to
prevent. Measured against the archived snapshots, the payload timestamp read
~45 seconds old while the newest trade in the entire chain was the PREVIOUS
SESSION'S CLOSE -- roughly 17 hours stale. Any UI that shows
`now - payload.timestamp` as "data age" is lying.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"
DEFAULT_SYMBOL = "_SPX"
TIMEOUT = 20
# Set by fetch() when the body is byte-identical to the LAST STORED snapshot
# (mark_stored), not merely the previous poll: a body whose storage failed must
# be stored on the next identical response, not skipped as "unchanged".
UNCHANGED = "unchanged since last stored snapshot"


@dataclass
class Snapshot:
    """One fetch, with provenance. `payload` is the raw decoded JSON."""
    payload: dict[str, Any]
    symbol: str
    received_at: datetime                 # clock 3: when we got it
    fetch_seconds: float
    http_status: int
    fingerprint: str                      # sha256 of raw bytes -- dedupe identical responses
    raw_bytes: int
    http_date: str | None = None          # CDN's own Date header
    http_age: str | None = None           # CDN Age header, if present
    retry_after: str | None = None        # Retry-After header (seconds or an HTTP date), if any
    warnings: list[str] = field(default_factory=list)
    # The exact bytes received, for the raw-every-poll window (store.
    # save_raw_poll). Kept off repr: ~13 MB for SPX.
    raw_body: bytes = field(default=b"", repr=False)

    @property
    def snapshot_time(self) -> datetime | None:
        """Clock 2. NOTE: this is publication time, NOT market-data age."""
        ts = self.payload.get("timestamp")
        if not ts:
            return None
        try:
            # No timezone offset in the string. Archived samples are consistent
            # with UTC for this field (contract `last_trade_time` looks like
            # Eastern) -- flagged in the original design notes as needing explicit validation.
            return datetime.fromisoformat(str(ts)).replace(tzinfo=timezone.utc)
        except ValueError:
            self.warnings.append(f"unparseable timestamp {ts!r}")
            return None

    @property
    def newest_trade_time(self) -> str | None:
        """Clock 1, the only one that reflects real market activity: the newest
        `last_trade_time` across every contract. This is what should drive any
        honest staleness indicator."""
        opts = self.payload.get("data", {}).get("options") or []
        best = None
        for o in opts:
            t = o.get("last_trade_time")
            if t and (best is None or t > best):
                best = t
        return best

    @property
    def spot(self) -> float | None:
        v = self.payload.get("data", {}).get("current_price")
        return float(v) if v is not None else None


class CboeClient:
    def __init__(self, symbol: str = DEFAULT_SYMBOL) -> None:
        self.symbol = symbol
        self._session = requests.Session()
        self._session.headers.update({
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "gex_tool/0.1 (personal research dashboard)",
        })
        self._stored_fingerprint: str | None = None

    def mark_stored(self, fingerprint: str) -> None:
        """The collector calls this once a snapshot's rows are durably stored;
        only then does an identical body count as "unchanged"."""
        self._stored_fingerprint = fingerprint

    def fetch(self) -> Snapshot:
        url = CBOE_URL.format(symbol=self.symbol)
        t0 = time.perf_counter()
        resp = self._session.get(url, timeout=TIMEOUT)
        elapsed = time.perf_counter() - t0
        received = datetime.now(timezone.utc)
        raw = resp.content
        fp = hashlib.sha256(raw).hexdigest()[:16]

        warnings: list[str] = []
        if resp.status_code != 200:
            warnings.append(f"http {resp.status_code}")
            payload: dict[str, Any] = {}
        else:
            try:
                payload = json.loads(raw)
            except ValueError as e:
                warnings.append(f"json decode failed: {e}")
                payload = {}
            # Valid JSON that is not an object (a list, a bare string) would
            # make every `payload.get` below raise -- before the raw-every-poll
            # window could keep the body.
            if not isinstance(payload, dict):
                warnings.append(f"payload is a JSON {type(payload).__name__}, not an object")
                payload = {}
            elif not isinstance(payload.get("data", {}), dict):
                warnings.append("payload 'data' is not an object")
                payload = {}

        # An identical body means the source has not refreshed. Worth surfacing
        # rather than silently re-rendering the same numbers: the archive
        # already contains one byte-identical weekend pair. Compared with the
        # last STORED body: comparing with the previous
        # poll -- updated before storage had succeeded -- meant a body whose
        # storage failed was skipped as "unchanged" on the next poll and lost.
        if fp == self._stored_fingerprint:
            warnings.append(UNCHANGED)

        snap = Snapshot(
            payload=payload, symbol=self.symbol, received_at=received,
            fetch_seconds=elapsed, http_status=resp.status_code, fingerprint=fp,
            raw_bytes=len(raw), http_date=resp.headers.get("Date"),
            http_age=resp.headers.get("Age"), retry_after=resp.headers.get("Retry-After"),
            warnings=warnings, raw_body=raw,
        )
        if payload and not payload.get("data", {}).get("options"):
            snap.warnings.append("payload has no options array")
        return snap


def save_raw(snap: Snapshot, directory) -> str:
    """Daily raw checkpoint, gzipped. NOT a per-poll archive -- see the storage
    section of the original design notes: raw every poll is ~1.29 TB/yr, gzipped daily is
    ~0.46 GB/yr.

    Published ATOMICALLY, same pattern as store._atomic_write_table: gzip into
    a `.tmp_` file in the same directory, fsync, then os.replace. Before
    2026-09-29 this wrote straight to the final name, so a crash mid-write left
    a truncated checkpoint that (a) suppressed that day's retry, since
    daily_checkpoint only checks the name, and (b) would be kept forever by
    the append-only backup. A `.tmp_*` leftover satisfies neither: the leading
    dot fails daily_checkpoint's prefix check, and the backup excludes it.
    """
    import os
    import tempfile
    from pathlib import Path
    d = Path(directory); d.mkdir(parents=True, exist_ok=True)
    stamp = snap.received_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    path = d / f"{snap.symbol.lstrip('_')}_{stamp}.json.gz"
    fd, tmp = tempfile.mkstemp(dir=str(d), prefix=".tmp_", suffix=".json.gz")
    os.close(fd)
    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as fh:
            json.dump(snap.payload, fh)
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return str(path)
