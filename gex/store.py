"""Three-layer persistence. See the storage section of the original design notes.

Storing raw responses every poll is untenable -- ~1.29 TB/year at 60s RTH
polling. The layers, cheapest-first:

  1. metadata   -- one row per poll. Provenance and quality, never the chain.
  2. aggregates -- one row per (strike, expiry, root) per poll, calls and
                   puts side by side (v4: ~325 KB/poll SPY, ~600 KB SPX).
                   This is what every panel actually reads.
  3. raw daily  -- ONE gzipped payload per session, ~0.46 GB/year. An audit
                   checkpoint, NOT a substitute for layer 2: it cannot
                   reconstruct minute-by-minute supplied gamma after the fact.

Spot is stored in the metadata rather than baked into the aggregates, so any
GEX/DEX unit convention can be derived later without a re-fetch. That is the
one decision here that would be expensive to reverse.

Schema note: aggregates retain BOTH the exact expiry and the bucket. The
bucket is what the UI filters on; the expiry is what makes historical
term-structure analysis possible. Storing only the bucket would have been
irreversible -- a bucket cannot be ungrouped, and CBOE offers no backfill.

Schema v2 (2026-09-29) adds per-side IV and bid/ask to the aggregates, and a
`schema_version` column to every aggregate row (v1 files have none; treat a
missing column as v1). Before v2 the only IV on disk was the ONE daily raw
checkpoint, which is taken at the first poll -- i.e. the previous session's
close, since the feed lags ~16 min. Intraday IV starts at the v2 deploy.

Schema v3 (2026-09-30) adds per-contract gamma and delta per side
(call_gamma, put_gamma, call_delta, put_delta), including zero-OI contracts.
Before v3 only the OI-weighted sums existed, so per-contract greeks were
recoverable as sum/(OI*100) only where OI > 0. Partitions <= 2026-09-28 are
v1, 2026-09-29 is v2, >= 2026-09-30 is v3 (deployed before that open).

Schema v4 (2026-10, for SPX) adds `root` (the OCC root) to every aggregate
row and to the row key: (strike, expiry, root). SPX lists AM-settled `SPX`
and PM-settled `SPXW` at the same strike on the same date; without the root
those pairs merged, their quotes/greeks were blanked as collisions and their
exposures summed. SPY rows carry root 'SPY'. v1-v3 files have no `root`
column (null when read with AGG_SCHEMA; every one of them is SPY).

READING ACROSS VERSION BOUNDARIES: pass an explicit schema (AGG_SCHEMA).
pyarrow.dataset infers the schema from the first file it opens, so a scan
that starts on a v1 file silently drops the v2 columns. Reading one file at
a time and concatenating with `promote_options="default"` also works.
"""
from __future__ import annotations

import gzip
import json
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:                                   # pragma: no cover
    pa = pq = None

SCHEMA_VERSION = 4

# Explicit, so a poll where some column happens to be all-null cannot write a
# `null`-typed column and drift the schema file to file. v1 columns keep the
# exact types v1 files were inferred with (checked against the deployed
# archive); the quote columns are nullable float64.
AGG_SCHEMA = None if pa is None else pa.schema([
    ("received_at", pa.string()),
    ("strike", pa.float64()),
    ("expiry", pa.string()),
    ("bucket", pa.string()),
    ("call_gamma_oi", pa.float64()), ("put_gamma_oi", pa.float64()),
    ("call_delta_oi", pa.float64()), ("put_delta_oi", pa.float64()),
    ("call_oi", pa.float64()), ("put_oi", pa.float64()),
    ("call_volume", pa.float64()), ("put_volume", pa.float64()),
    ("call_iv", pa.float64()), ("put_iv", pa.float64()),
    ("call_bid", pa.float64()), ("call_ask", pa.float64()),
    ("put_bid", pa.float64()), ("put_ask", pa.float64()),
    ("call_gamma", pa.float64()), ("put_gamma", pa.float64()),
    ("call_delta", pa.float64()), ("put_delta", pa.float64()),
    ("schema_version", pa.int64()),
    ("root", pa.string()),                        # v4
])


def _require_arrow() -> None:
    if pa is None:
        raise RuntimeError("pyarrow required for storage; pip install pyarrow")


def _atomic_write_table(table, path: Path) -> Path:
    """Write Parquet atomically.

    Writing straight to the final name leaves an unreadable, footer-less file
    if the process dies mid-write -- and one corrupt file can break a whole
    partition scan, not just its own poll. Write to a temp file in the SAME
    directory (so os.replace stays on one filesystem and is atomic), fsync,
    then rename. A reader therefore only ever sees a complete file.
    """
    _require_arrow()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".parquet")
    os.close(fd)
    try:
        pq.write_table(table, tmp, compression="zstd")
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def _stamp(dt: datetime) -> str:
    """Microsecond-resolution filename stamp. HHMMSS alone silently overwrites
    when two receipts land in the same second (a sequential restart can do
    this), pairing one poll's metadata with another's aggregates."""
    return dt.strftime("%H%M%S_%f")


def _day_dir(root: Path, symbol: str, d: date) -> Path:
    p = root / f"symbol={symbol.lstrip('_')}" / f"date={d.isoformat()}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_metadata(root: Path, snap, agg: dict, net: dict) -> Path:
    """Layer 1. One row per poll: everything needed to judge whether a later
    reader should trust the aggregate written alongside it."""
    _require_arrow()
    root = Path(root)
    d = _day_dir(root / "metadata", snap.symbol, snap.received_at.date())
    row = {
        "received_at": snap.received_at.isoformat(),
        "snapshot_time": snap.snapshot_time.isoformat() if snap.snapshot_time else None,
        "newest_trade_time": snap.newest_trade_time,      # the ONLY honest staleness input
        "spot": agg.get("spot"),
        "fetch_seconds": snap.fetch_seconds,
        "http_status": snap.http_status,
        "raw_bytes": snap.raw_bytes,
        "fingerprint": snap.fingerprint,
        "n_contracts": agg.get("n_contracts"),
        "n_rows": agg.get("n_rows"),
        "n_unparseable": agg.get("n_unparseable"),
        "n_quote_collisions": agg.get("n_quote_collisions"),
        "n_am_expiring_missing_greeks": agg.get("n_am_expiring_missing_greeks"),   # v4
        "usable": agg.get("usable", False),
        "problems": json.dumps(agg.get("problems", []) + snap.warnings),
        "net_gex_per_pct": net.get("net_gex"),
        "net_dex": net.get("net_dex"),
        "schema_version": SCHEMA_VERSION,
    }
    return _atomic_write_table(pa.Table.from_pylist([row]),
                               d / f"meta_{_stamp(snap.received_at)}.parquet")


def append_aggregate(root: Path, snap, agg: dict) -> Path | None:
    """Layer 2. The per-strike rows every panel reads.

    Deliberately stores the SPOT-UNSCALED sums (`gamma*OI*100`, `delta*OI*100`)
    rather than a finished GEX number, so the display unit stays a rendering
    decision rather than a storage decision.
    """
    _require_arrow()
    rows = agg.get("rows") or []
    if not rows:
        return None
    root = Path(root)
    d = _day_dir(root / "aggregates", snap.symbol, snap.received_at.date())
    stamp = snap.received_at.isoformat()
    payload = [{
        "received_at": stamp,
        "strike": r["strike"],
        "expiry": r["expiry"],        # exact date -- the irreversible choice,
        "bucket": r["bucket"],        # made deliberately; see aggregate.py
        "call_gamma_oi": r["call_gamma_oi"], "put_gamma_oi": r["put_gamma_oi"],
        "call_delta_oi": r["call_delta_oi"], "put_delta_oi": r["put_delta_oi"],
        "call_oi": r["call_oi"], "put_oi": r["put_oi"],
        "call_volume": r["call_volume"], "put_volume": r["put_volume"],
        "call_iv": r.get("call_iv"), "put_iv": r.get("put_iv"),
        "call_bid": r.get("call_bid"), "call_ask": r.get("call_ask"),
        "put_bid": r.get("put_bid"), "put_ask": r.get("put_ask"),
        "call_gamma": r.get("call_gamma"), "put_gamma": r.get("put_gamma"),
        "call_delta": r.get("call_delta"), "put_delta": r.get("put_delta"),
        "schema_version": SCHEMA_VERSION,
        "root": r["root"],            # v4: part of the row key -- a KeyError
                                      # here beats silently storing null
    } for r in rows]
    return _atomic_write_table(pa.Table.from_pylist(payload, schema=AGG_SCHEMA),
                               d / f"agg_{_stamp(snap.received_at)}.parquet")


def daily_checkpoint(root: Path, snap, force: bool = False) -> Path | None:
    """Layer 3. At most one raw gzipped payload per symbol per day."""
    from .fetch import save_raw
    root = Path(root)
    d = root / "raw" / f"symbol={snap.symbol.lstrip('_')}"
    d.mkdir(parents=True, exist_ok=True)
    today = snap.received_at.date().isoformat()
    if not force and any(p.name.startswith(f"{snap.symbol.lstrip('_')}_{today}") for p in d.iterdir()):
        return None                                   # already have today's
    # save_raw publishes atomically (temp file + rename), so a partial file can
    # neither exist under the final name nor suppress the day's retry by its
    # presence. (Until 2026-09-29 this comment claimed a rename that the code
    # did not do -- the "rename" line was a no-op.)
    return Path(save_raw(snap, d))


def _atomic_write_bytes(data: bytes, path: Path) -> Path:
    """Same publish pattern as _atomic_write_table, for an opaque byte string."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=path.suffix)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def save_raw_poll(root: Path, snap, session: date) -> Path:
    """Raw-every-poll window (schema v4 transition, SPX). Keeps the EXACT bytes
    of every response received -- usable, unusable, unchanged or an HTTP error
    alike -- so a capture rule found wrong later can be re-run without a
    backfill CBOE does not offer. Called BEFORE aggregation, which can raise.

    Not deduplicated: an A -> B -> A sequence must keep both A receipts, and
    the cost is the same as saving every poll (~1.8 MB gzipped per SPX poll).

    Layout: raw_polls/symbol=X/date=<UTC date>/X_<HHMMSS_ffffff>.json.gz plus
    a sidecar .json written AFTER the body: the sidecar's existence marks a
    complete record. Replays must take the session date from the sidecar,
    never from the replay day.
    """
    root = Path(root)
    sym = snap.symbol.lstrip("_")
    d = _day_dir(root / "raw_polls", snap.symbol, snap.received_at.date())
    base = f"{sym}_{_stamp(snap.received_at)}"
    body = d / f"{base}.json.gz"
    _atomic_write_bytes(gzip.compress(snap.raw_body, compresslevel=6), body)
    receipt = {
        "symbol": snap.symbol,
        "received_at": snap.received_at.isoformat(),
        "session_date": session.isoformat(),
        "fingerprint": snap.fingerprint,
        "http_status": snap.http_status,
        "http_date": snap.http_date,
        "http_age": snap.http_age,
        "retry_after": snap.retry_after,
        "fetch_seconds": snap.fetch_seconds,
        "raw_bytes": snap.raw_bytes,
        "fetch_warnings": list(snap.warnings),
        "body": body.name,
    }
    _atomic_write_bytes(json.dumps(receipt, sort_keys=True).encode(), d / f"{base}.json")
    return body
