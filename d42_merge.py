#!/usr/bin/env python3
"""
d42_merge.py — refresh local Device42 cache and merge NEW rows into
~/console_db/console_devices.csv and ~/console_db/pdu_mapping.json.

Designed to be invoked as fire-and-forget from console.py.

Sync rule (deliberate):
    * INSERT entries that exist in Device42 but not in the local files.
    * NEVER overwrite, edit, or delete an existing local entry, even if
      Device42 disagrees. Disagreements are recorded in
      /home/dn/console_db/d42_merge.log so a human can review.

Throttle: by default skips the merge if it ran in the last 5 minutes.
Pass --force to override.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import fcntl
import json
import logging
import logging.handlers
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Default to the directory this script lives in, so a `git clone` anywhere
# works out of the box. Override via env vars when running on the dev VM.
CONSOLE_DB_DIR = Path(os.environ.get(
    "CONSOLE_DB_DIR", str(Path(__file__).resolve().parent)
))
CSV_PATH = Path(os.environ.get("CONSOLE_CSV_PATH",
                               str(CONSOLE_DB_DIR / "console_devices.csv")))
JSON_PATH = Path(os.environ.get("PDU_MAP_PATH",
                                str(CONSOLE_DB_DIR / "pdu_mapping.json")))
LOG_PATH = CONSOLE_DB_DIR / "d42_merge.log"
LOCK_PATH = CONSOLE_DB_DIR / ".d42_merge.lock"
STAMP_PATH = CONSOLE_DB_DIR / ".d42_merge.last"

SQLITE_CACHE = Path(os.environ.get(
    "DEVICE42_CACHE_PATH",
    str(Path.home() / "device42_cache.sqlite"),
))
SYNC_SCRIPT = CONSOLE_DB_DIR / "dump_d42_consoles.py"

DEFAULT_THROTTLE_SEC = 5 * 60  # don't re-sync more than once per 5 min


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    log = logging.getLogger("d42_merge")
    log.setLevel(logging.INFO)
    if not log.handlers:
        handler = logging.handlers.RotatingFileHandler(
            LOG_PATH, maxBytes=2_000_000, backupCount=3
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-7s %(message)s",
                              datefmt="%Y-%m-%dT%H:%M:%S")
        )
        log.addHandler(handler)
    return log


# ---------------------------------------------------------------------------
# Lock + throttle
# ---------------------------------------------------------------------------

class _Lock:
    """Non-blocking file lock; if another merge is in progress, skip this run."""

    def __init__(self, path: Path):
        self.path = path
        self.fh: Optional[object] = None

    def __enter__(self):
        self.fh = open(self.path, "w")
        try:
            fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.fh.close()
            self.fh = None
            return None
        return self

    def __exit__(self, *_):
        if self.fh is not None:
            try:
                fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
            finally:
                self.fh.close()


def _throttled(throttle_sec: int) -> bool:
    if not STAMP_PATH.exists():
        return False
    try:
        age = time.time() - STAMP_PATH.stat().st_mtime
    except OSError:
        return False
    return age < throttle_sec


def _stamp_now() -> None:
    STAMP_PATH.write_text(_dt.datetime.now().isoformat(timespec="seconds"))


# ---------------------------------------------------------------------------
# Run the SQLite-cache sync (delegates to dump_d42_consoles.py)
# ---------------------------------------------------------------------------

def _refresh_sqlite_cache(log: logging.Logger) -> bool:
    if not SYNC_SCRIPT.exists():
        log.error("sync script missing: %s", SYNC_SCRIPT)
        return False
    try:
        proc = subprocess.run(
            [sys.executable, str(SYNC_SCRIPT), "sync", "--no-txt"],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode != 0:
            log.error("d42 sync failed (rc=%s): %s", proc.returncode,
                      proc.stderr.strip())
            return False
        for line in proc.stdout.strip().splitlines():
            log.info("sync: %s", line)
        return True
    except subprocess.TimeoutExpired:
        log.error("d42 sync timed out")
        return False


# ---------------------------------------------------------------------------
# Console CSV merge
# ---------------------------------------------------------------------------

# Strict parse: "Console<N> @ console-<X>" (case-insensitive).
# Anything else (m-wb-console*, ilo names, free text) is logged as
# 'needs_review' and never auto-added to the CSV.
_CONSOLE_RE = re.compile(
    r"^Console\s*(\d+)\s*@\s*(console-[A-Za-z0-9-]+)\s*$",
    re.IGNORECASE,
)


def _normalize_console_server(name: str) -> str:
    s = name.strip().upper()
    if not s.startswith("CONSOLE-"):
        s = "CONSOLE-" + s
    return s


def _read_csv() -> tuple[list[str], list[list[str]], dict[str, list[str]]]:
    if not CSV_PATH.exists():
        return ["Console Server", "Port #", "Device Serial"], [], {}
    rows: list[list[str]] = []
    with CSV_PATH.open(newline="") as f:
        rdr = csv.reader(f)
        try:
            header = next(rdr)
        except StopIteration:
            header = ["Console Server", "Port #", "Device Serial"]
        for row in rdr:
            if not row:
                continue
            rows.append(row)
    by_serial: dict[str, list[str]] = {
        r[2].strip().upper(): r for r in rows if len(r) >= 3 and r[2].strip()
    }
    return header, rows, by_serial


def _write_csv(header: list[str], rows: list[list[str]]) -> None:
    tmp = CSV_PATH.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    tmp.replace(CSV_PATH)


def _merge_consoles(conn: sqlite3.Connection, log: logging.Logger,
                    dry_run: bool) -> dict:
    counts = {"existing": 0, "added": 0, "mismatch": 0, "needs_review": 0,
              "skipped_empty_serial": 0}

    header, rows, by_serial = _read_csv()
    new_rows: list[list[str]] = []

    for device, console_str in conn.execute(
        "SELECT device, console FROM consoles ORDER BY device"
    ):
        # CSV uses *serial* as key; D42 hostnames may be free-form.
        # Use the leading token before the first space as the serial.
        serial = (device or "").strip().split()[0].upper()
        if not serial:
            counts["skipped_empty_serial"] += 1
            continue

        m = _CONSOLE_RE.match(console_str or "")
        if not m:
            if serial not in by_serial:
                counts["needs_review"] += 1
                log.info("needs_review console: serial=%s d42=%r "
                         "(unparseable; not added)", serial, console_str)
            continue

        d42_port = m.group(1)
        d42_server = _normalize_console_server(m.group(2))

        existing = by_serial.get(serial)
        if existing is None:
            new_rows.append([d42_server, d42_port, serial])
            counts["added"] += 1
            log.info("add console: %s -> %s port %s",
                     serial, d42_server, d42_port)
            continue

        cur_server = existing[0].strip().upper() if len(existing) >= 1 else ""
        cur_port = existing[1].strip() if len(existing) >= 2 else ""
        if cur_server == d42_server and cur_port == d42_port:
            counts["existing"] += 1
        else:
            counts["mismatch"] += 1
            log.info("mismatch console: serial=%s csv=(%s, %s) d42=(%s, %s) "
                     "(left as-is)", serial, cur_server, cur_port,
                     d42_server, d42_port)

    if new_rows and not dry_run:
        rows.extend(new_rows)
        _write_csv(header, rows)
    return counts


# ---------------------------------------------------------------------------
# PDU JSON merge
# ---------------------------------------------------------------------------

def _normalize_pdu_name(name: str) -> str:
    s = (name or "").strip().lower()
    if not s:
        return s
    if not s.startswith("pdu-") and not s.startswith("m-wb-power"):
        s = "pdu-" + s
    return s


def _translate_outlet(raw: str) -> Optional[int]:
    """Mirror parse_pdu_info() in dnos_e2e_remote_utils.py."""
    s = (raw or "").strip()
    if not s:
        return None
    if s[0].isalpha():
        prefix = s[0].upper()
        rest = s[1:]
        if not rest.isdigit():
            return None
        n = int(rest)
        if prefix == "B":
            return n + 12
        # 'A' (and any other letter) -> just drop the letter
        return n
    if s.isdigit():
        return int(s)
    return None


def _read_json() -> dict:
    if not JSON_PATH.exists():
        return {}
    try:
        with JSON_PATH.open() as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}


def _write_json(data: dict) -> None:
    tmp = JSON_PATH.with_suffix(".json.tmp")
    with tmp.open("w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    tmp.replace(JSON_PATH)


def _entry_list(value) -> list[dict]:
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _merge_pdus(conn: sqlite3.Connection, log: logging.Logger,
                dry_run: bool) -> dict:
    counts = {"existing": 0, "added": 0, "mismatch": 0,
              "skipped_unparseable_outlet": 0, "skipped_empty_serial": 0}

    data = _read_json()
    changed = False

    for device, pdu, outlet_raw, _model in conn.execute(
        "SELECT device, pdu, outlet, model FROM pdus ORDER BY device, pdu"
    ):
        serial = (device or "").strip().split()[0].upper()
        if not serial:
            counts["skipped_empty_serial"] += 1
            continue

        outlet = _translate_outlet(outlet_raw or "")
        if outlet is None:
            counts["skipped_unparseable_outlet"] += 1
            log.info("skip pdu (bad outlet): serial=%s pdu=%s outlet=%r",
                     serial, pdu, outlet_raw)
            continue

        pdu_norm = _normalize_pdu_name(pdu)
        new_entry = {"pdu": pdu_norm, "outlet": outlet}

        existing_entries = _entry_list(data.get(serial))
        # Match on PDU name; outlet may differ -> mismatch (don't fix).
        idx = next(
            (i for i, e in enumerate(existing_entries)
             if (e.get("pdu") or "").lower() == pdu_norm),
            None,
        )

        if idx is None:
            existing_entries.append(new_entry)
            data[serial] = (
                existing_entries[0] if len(existing_entries) == 1
                else existing_entries
            )
            counts["added"] += 1
            changed = True
            log.info("add pdu: %s -> %s outlet %s",
                     serial, pdu_norm, outlet)
            continue

        cur = existing_entries[idx]
        cur_outlet = cur.get("outlet")
        try:
            cur_outlet_int = int(cur_outlet)
        except (TypeError, ValueError):
            cur_outlet_int = cur_outlet

        if cur_outlet_int == outlet:
            counts["existing"] += 1
        else:
            counts["mismatch"] += 1
            log.info("mismatch pdu: serial=%s pdu=%s json_outlet=%r "
                     "d42_outlet=%s (left as-is)",
                     serial, pdu_norm, cur_outlet, outlet)

    if changed and not dry_run:
        _write_json(data)
    return counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--force", action="store_true",
                   help="Ignore the throttle window")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute changes but do not write CSV/JSON")
    p.add_argument("--throttle", type=int, default=DEFAULT_THROTTLE_SEC,
                   help=f"Throttle window in seconds (default {DEFAULT_THROTTLE_SEC})")
    args = p.parse_args()

    CONSOLE_DB_DIR.mkdir(parents=True, exist_ok=True)
    log = _setup_logging()

    if not args.force and _throttled(args.throttle):
        log.info("skipped (throttled, last run < %ss ago)", args.throttle)
        return 0

    with _Lock(LOCK_PATH) as got:
        if got is None:
            log.info("skipped (another merge is already running)")
            return 0

        log.info("merge start (dry_run=%s, force=%s)", args.dry_run, args.force)

        if not _refresh_sqlite_cache(log):
            log.error("merge aborted: sqlite cache refresh failed")
            return 1

        try:
            with sqlite3.connect(SQLITE_CACHE) as conn:
                console_counts = _merge_consoles(conn, log, args.dry_run)
                pdu_counts = _merge_pdus(conn, log, args.dry_run)
        except sqlite3.Error as e:
            log.error("sqlite error: %s", e)
            return 1

        log.info("consoles: %s", console_counts)
        log.info("pdus:     %s", pdu_counts)
        if not args.dry_run:
            _stamp_now()
        log.info("merge done")

    return 0


if __name__ == "__main__":
    sys.exit(main())
