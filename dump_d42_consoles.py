"""Device42 console + PDU local cache.

Maintains a local SQLite database (~/device42_cache.sqlite by default) that
mirrors the console-server and PDU mappings stored in Device42, and produces
plain-text dumps for human inspection.

Sync rules (deliberate, per request):

    - INSERT new rows that exist in Device42 but not in the local DB.
    - NEVER overwrite an existing local row, even if Device42 disagrees.
        Mismatches are recorded in the `sync_log` table for review.
        Rationale: a row may have been edited manually on purpose because the
        Device42 entry was wrong; the cache must not silently revert that.
    - Rows that disappear from Device42 are kept as well (logged as 'orphan').

Sources of data: the same DOQL endpoint and queries used by
tests/shared/dnos_e2e_utils/dnos_e2e_remote_utils.py.

Usage::

    # First time / refresh:
    python3 dump_d42_consoles.py sync               # both consoles + pdus
    python3 dump_d42_consoles.py sync consoles      # only consoles
    python3 dump_d42_consoles.py sync pdus          # only pdus

    # Write text dumps from the cache (no network):
    python3 dump_d42_consoles.py dump
    python3 dump_d42_consoles.py dump consoles
    python3 dump_d42_consoles.py dump pdus

    # Inspect what changed during the last syncs:
    python3 dump_d42_consoles.py log               # last 50 events
    python3 dump_d42_consoles.py log mismatch      # filter by action

    # Hand-edit a row and mark it manual:
    python3 dump_d42_consoles.py mark-manual consoles WDY1C3VS00090
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import sqlite3
import sys
import warnings
from pathlib import Path
from typing import Iterable, Optional, Sequence

import requests
import urllib3

warnings.filterwarnings("ignore", category=DeprecationWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _load_env_file(path="~/.console_env"):
    """Source 'export KEY=value' lines from the given file into os.environ.
    Existing env values win (setdefault). Lets the script work under SSH
    non-interactive shells where ~/.bashrc is not sourced."""
    p = os.path.expanduser(path)
    try:
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export "):]
                if "=" not in line:
                    continue
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except (OSError, FileNotFoundError):
        pass


_load_env_file()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Device42 endpoint + Basic-auth header come from environment variables so
# this file can live in a public repo. Set them in ~/.console_env (see README).
ENDPOINT = os.environ.get(
    "DEVICE42_ENDPOINT",
    "https://device42.example.com/services/data/v1.0/query/",
)
HEADER = {"Authorization": os.environ.get("DEVICE42_AUTH", "")}
TIMEOUT = 60

DEFAULT_DB_PATH = Path.home() / "device42_cache.sqlite"
DEFAULT_CONSOLES_TXT = Path.home() / "device42_consoles.txt"
DEFAULT_PDUS_TXT = Path.home() / "device42_pdus.txt"

CONSOLE_QUERY = (
    "select sd.name, tp.verbose_name "
    "from view_netport_v1 tp "
    "left join view_netport_v1 sp on (tp.netport_pk = sp.remote_netport_fk "
    "or tp.remote_netport_fk = sp.netport_pk) "
    "left join view_device_v2 sd on sd.device_pk = sp.device_fk "
    "where (sp.port like '%Console%' or sp.port like '%console%' "
    "or sp.port like '%Console0%' or sp.port like '%console0%') "
    "and sd.name is not null "
    "order by sd.name"
)

PDU_QUERY = (
    "select d.name, pdu.name, pp.port_name, pm.name "
    "from view_pduports_v1 pp "
    "left join view_pdu_v1 pdu on pp.pdu_fk = pdu.pdu_pk "
    "left join view_device_v2 d on d.device_pk = pp.psu_device_fk "
    "left join view_pdumodel_v1 pm on pdu.pdumodel_fk = pm.pdumodel_pk "
    "where d.name is not null "
    "order by d.name, pdu.name, pp.port_name"
)

SOURCE_DEVICE42 = "device42"
SOURCE_MANUAL = "manual"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS consoles (
    device      TEXT PRIMARY KEY,
    console     TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'device42',
    first_seen  TEXT NOT NULL,
    last_synced TEXT NOT NULL,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS pdus (
    device      TEXT NOT NULL,
    pdu         TEXT NOT NULL,
    outlet      TEXT,
    model       TEXT,
    source      TEXT NOT NULL DEFAULT 'device42',
    first_seen  TEXT NOT NULL,
    last_synced TEXT NOT NULL,
    notes       TEXT,
    PRIMARY KEY (device, pdu)
);

CREATE TABLE IF NOT EXISTS sync_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    action      TEXT NOT NULL,  -- insert | unchanged | mismatch | orphan | error
    device      TEXT,
    details     TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_log_ts        ON sync_log(ts);
CREATE INDEX IF NOT EXISTS idx_sync_log_action    ON sync_log(action);
CREATE INDEX IF NOT EXISTS idx_sync_log_table     ON sync_log(table_name);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _open_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn


def _fetch(query: str) -> str:
    if not HEADER.get("Authorization"):
        raise RuntimeError(
            "DEVICE42_AUTH is not set. Export it as a Basic-auth header, e.g.:\n"
            "  export DEVICE42_AUTH=\"Basic $(printf '%s' user:pass | base64)\"\n"
            "  export DEVICE42_ENDPOINT=\"https://your-device42/services/data/v1.0/query/\""
        )
    resp = requests.get(
        ENDPOINT,
        headers=HEADER,
        params={"query": query},
        verify=False,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.text


def _split_csv_line(line: str, max_fields: int) -> list[str]:
    """Like line.split(',', max_fields-1) but stripped and right-padded."""
    parts = [p.strip() for p in line.split(",", max_fields - 1)]
    while len(parts) < max_fields:
        parts.append("")
    return parts


def _log(conn: sqlite3.Connection, table: str, action: str,
         device: Optional[str], details: str) -> None:
    conn.execute(
        "INSERT INTO sync_log(ts, table_name, action, device, details) "
        "VALUES (?, ?, ?, ?, ?)",
        (_now_iso(), table, action, device, details),
    )


# ---------------------------------------------------------------------------
# Sync: consoles
# ---------------------------------------------------------------------------

def sync_consoles(conn: sqlite3.Connection) -> dict:
    raw = _fetch(CONSOLE_QUERY).strip()
    fresh: dict[str, str] = {}
    for line in raw.splitlines():
        device, console = _split_csv_line(line, 2)
        if not device or not console:
            continue
        # Only the first row per device wins; D42 sometimes returns duplicates.
        fresh.setdefault(device, console)

    counts = {"insert": 0, "unchanged": 0, "mismatch": 0, "orphan": 0}
    now = _now_iso()

    for device, console in fresh.items():
        row = conn.execute(
            "SELECT console, source FROM consoles WHERE device = ?",
            (device,),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO consoles(device, console, source, first_seen, last_synced) "
                "VALUES (?, ?, ?, ?, ?)",
                (device, console, SOURCE_DEVICE42, now, now),
            )
            _log(conn, "consoles", "insert", device, f"console={console!r}")
            counts["insert"] += 1
        else:
            cached_console, cached_source = row
            if cached_console == console:
                conn.execute(
                    "UPDATE consoles SET last_synced = ? WHERE device = ?",
                    (now, device),
                )
                counts["unchanged"] += 1
            else:
                # NEVER overwrite. Log so a human can decide.
                conn.execute(
                    "UPDATE consoles SET last_synced = ? WHERE device = ?",
                    (now, device),
                )
                _log(
                    conn, "consoles", "mismatch", device,
                    f"local={cached_console!r} (source={cached_source}) "
                    f"vs device42={console!r}",
                )
                counts["mismatch"] += 1

    # Detect rows that exist locally but no longer come from D42.
    cached_devices = {r[0] for r in conn.execute("SELECT device FROM consoles")}
    for device in cached_devices - fresh.keys():
        _log(conn, "consoles", "orphan", device, "no longer returned by Device42")
        counts["orphan"] += 1

    conn.commit()
    return counts


# ---------------------------------------------------------------------------
# Sync: pdus
# ---------------------------------------------------------------------------

def sync_pdus(conn: sqlite3.Connection) -> dict:
    raw = _fetch(PDU_QUERY).strip()
    # Key: (device, pdu) -> (outlet, model)
    fresh: dict[tuple[str, str], tuple[str, str]] = {}
    for line in raw.splitlines():
        device, pdu, outlet, model = _split_csv_line(line, 4)
        if not device or not pdu:
            continue
        fresh.setdefault((device, pdu), (outlet, model))

    counts = {"insert": 0, "unchanged": 0, "mismatch": 0, "orphan": 0}
    now = _now_iso()

    for (device, pdu), (outlet, model) in fresh.items():
        row = conn.execute(
            "SELECT outlet, model, source FROM pdus WHERE device = ? AND pdu = ?",
            (device, pdu),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO pdus(device, pdu, outlet, model, source, "
                "first_seen, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (device, pdu, outlet, model, SOURCE_DEVICE42, now, now),
            )
            _log(
                conn, "pdus", "insert", device,
                f"pdu={pdu!r} outlet={outlet!r} model={model!r}",
            )
            counts["insert"] += 1
        else:
            cached_outlet, cached_model, cached_source = row
            if cached_outlet == outlet and cached_model == model:
                conn.execute(
                    "UPDATE pdus SET last_synced = ? WHERE device = ? AND pdu = ?",
                    (now, device, pdu),
                )
                counts["unchanged"] += 1
            else:
                conn.execute(
                    "UPDATE pdus SET last_synced = ? WHERE device = ? AND pdu = ?",
                    (now, device, pdu),
                )
                _log(
                    conn, "pdus", "mismatch", device,
                    f"pdu={pdu!r} local=(outlet={cached_outlet!r}, model={cached_model!r}, "
                    f"source={cached_source}) "
                    f"vs device42=(outlet={outlet!r}, model={model!r})",
                )
                counts["mismatch"] += 1

    cached_keys = {
        (r[0], r[1]) for r in conn.execute("SELECT device, pdu FROM pdus")
    }
    for device, pdu in cached_keys - fresh.keys():
        _log(conn, "pdus", "orphan", device,
             f"pdu={pdu!r} no longer returned by Device42")
        counts["orphan"] += 1

    conn.commit()
    return counts


# ---------------------------------------------------------------------------
# Dumps (text files generated from the local DB)
# ---------------------------------------------------------------------------

def dump_consoles(conn: sqlite3.Connection, out_path: Path) -> int:
    rows = list(conn.execute(
        "SELECT device, console, source, first_seen, last_synced, notes "
        "FROM consoles ORDER BY LOWER(device)"
    ))
    if not rows:
        print(f"No console rows in DB; run 'sync consoles' first.", file=sys.stderr)
        return 1

    dev_w = max(len(r[0]) for r in rows)
    cons_w = max(len(r[1]) for r in rows)
    src_w = max(len(r[2]) for r in rows)

    with out_path.open("w") as f:
        f.write(f"# Console connections from local cache ({len(rows)} entries)\n")
        f.write(f"# Source DB: see device42_cache.sqlite (synced from {ENDPOINT})\n")
        f.write("# 'source' column: 'device42' = imported, 'manual' = hand-edited (do not auto-fix)\n")
        f.write("#\n")
        f.write(
            f"{'DEVICE'.ljust(dev_w)}  {'CONSOLE'.ljust(cons_w)}  "
            f"{'SOURCE'.ljust(src_w)}  LAST_SYNCED              NOTES\n"
        )
        f.write(
            f"{'-'*dev_w}  {'-'*cons_w}  {'-'*src_w}  "
            f"{'-'*24}  -----\n"
        )
        for device, console, source, _first, last_synced, notes in rows:
            f.write(
                f"{device.ljust(dev_w)}  {console.ljust(cons_w)}  "
                f"{source.ljust(src_w)}  {last_synced:<24}  {notes or ''}\n"
            )

    print(f"Wrote {len(rows)} console entries to {out_path}")
    return 0


def dump_pdus(conn: sqlite3.Connection, out_path: Path) -> int:
    rows = list(conn.execute(
        "SELECT device, pdu, outlet, model, source, last_synced, notes "
        "FROM pdus ORDER BY LOWER(device), LOWER(pdu)"
    ))
    if not rows:
        print(f"No PDU rows in DB; run 'sync pdus' first.", file=sys.stderr)
        return 1

    def _w(idx: int, default: int = 4) -> int:
        return max((len(str(r[idx]) or "") for r in rows), default=default)

    dev_w = _w(0); pdu_w = _w(1); out_w = max(_w(2), 6); mdl_w = max(_w(3), 5)
    src_w = max(_w(4), 6)

    with out_path.open("w") as f:
        f.write(f"# PDU connections from local cache ({len(rows)} entries)\n")
        f.write(f"# Source DB: see device42_cache.sqlite (synced from {ENDPOINT})\n")
        f.write("# 'source' column: 'device42' = imported, 'manual' = hand-edited (do not auto-fix)\n")
        f.write("# Columns: DEVICE  PDU  OUTLET  MODEL  SOURCE  LAST_SYNCED  NOTES\n")
        f.write("#\n")
        f.write(
            f"{'DEVICE'.ljust(dev_w)}  {'PDU'.ljust(pdu_w)}  "
            f"{'OUTLET'.ljust(out_w)}  {'MODEL'.ljust(mdl_w)}  "
            f"{'SOURCE'.ljust(src_w)}  LAST_SYNCED              NOTES\n"
        )
        f.write(
            f"{'-'*dev_w}  {'-'*pdu_w}  {'-'*out_w}  {'-'*mdl_w}  "
            f"{'-'*src_w}  {'-'*24}  -----\n"
        )
        for device, pdu, outlet, model, source, last_synced, notes in rows:
            f.write(
                f"{device.ljust(dev_w)}  {pdu.ljust(pdu_w)}  "
                f"{(outlet or '').ljust(out_w)}  {(model or '').ljust(mdl_w)}  "
                f"{source.ljust(src_w)}  {last_synced:<24}  {notes or ''}\n"
            )

    print(f"Wrote {len(rows)} PDU entries to {out_path}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_counts(label: str, counts: dict) -> None:
    parts = ", ".join(f"{k}={v}" for k, v in counts.items())
    print(f"  {label}: {parts}")


def cmd_sync(args: argparse.Namespace) -> int:
    conn = _open_db(args.db)
    try:
        targets = ["consoles", "pdus"] if args.what == "all" else [args.what]
        if "consoles" in targets:
            print("Syncing consoles from Device42 ...")
            _print_counts("consoles", sync_consoles(conn))
        if "pdus" in targets:
            print("Syncing PDUs from Device42 ...")
            _print_counts("pdus", sync_pdus(conn))

        # Always rewrite the txt dumps after a sync, so .txt + .sqlite agree.
        if args.write_txt:
            if "consoles" in targets:
                dump_consoles(conn, args.consoles_txt)
            if "pdus" in targets:
                dump_pdus(conn, args.pdus_txt)
        return 0
    finally:
        conn.close()


def cmd_dump(args: argparse.Namespace) -> int:
    conn = _open_db(args.db)
    try:
        rc = 0
        targets = ["consoles", "pdus"] if args.what == "all" else [args.what]
        if "consoles" in targets:
            rc |= dump_consoles(conn, args.consoles_txt)
        if "pdus" in targets:
            rc |= dump_pdus(conn, args.pdus_txt)
        return rc
    finally:
        conn.close()


def cmd_log(args: argparse.Namespace) -> int:
    conn = _open_db(args.db)
    try:
        sql = "SELECT ts, table_name, action, device, details FROM sync_log"
        params: list = []
        clauses: list[str] = []
        if args.action:
            clauses.append("action = ?")
            params.append(args.action)
        if args.table:
            clauses.append("table_name = ?")
            params.append(args.table)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(args.limit)
        for ts, table, action, device, details in conn.execute(sql, params):
            print(f"{ts}  {table:<8}  {action:<9}  {device or '-':<35}  {details or ''}")
        return 0
    finally:
        conn.close()


def cmd_mark_manual(args: argparse.Namespace) -> int:
    conn = _open_db(args.db)
    try:
        if args.what == "consoles":
            cur = conn.execute(
                "UPDATE consoles SET source = ?, notes = COALESCE(?, notes) "
                "WHERE device = ?",
                (SOURCE_MANUAL, args.note, args.device),
            )
        else:
            cur = conn.execute(
                "UPDATE pdus SET source = ?, notes = COALESCE(?, notes) "
                "WHERE device = ? AND (? IS NULL OR pdu = ?)",
                (SOURCE_MANUAL, args.note, args.device, args.pdu, args.pdu),
            )
        if cur.rowcount == 0:
            print(f"No matching row in {args.what}", file=sys.stderr)
            return 1
        conn.commit()
        print(f"Marked {cur.rowcount} row(s) in {args.what} as manual.")
        return 0
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", type=Path, default=DEFAULT_DB_PATH,
                   help=f"SQLite cache path (default: {DEFAULT_DB_PATH})")
    p.add_argument("--consoles-txt", type=Path, default=DEFAULT_CONSOLES_TXT,
                   help=f"Console dump path (default: {DEFAULT_CONSOLES_TXT})")
    p.add_argument("--pdus-txt", type=Path, default=DEFAULT_PDUS_TXT,
                   help=f"PDU dump path (default: {DEFAULT_PDUS_TXT})")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp_sync = sub.add_parser("sync", help="Sync DB from Device42")
    sp_sync.add_argument("what", nargs="?", default="all",
                        choices=["all", "consoles", "pdus"])
    sp_sync.add_argument("--no-txt", dest="write_txt", action="store_false",
                        default=True,
                        help="Do not rewrite the .txt dumps after sync")
    sp_sync.set_defaults(func=cmd_sync)

    sp_dump = sub.add_parser("dump", help="Write .txt dumps from the local DB")
    sp_dump.add_argument("what", nargs="?", default="all",
                        choices=["all", "consoles", "pdus"])
    sp_dump.set_defaults(func=cmd_dump)

    sp_log = sub.add_parser("log", help="Show sync_log entries")
    sp_log.add_argument("action", nargs="?",
                        choices=["insert", "unchanged", "mismatch", "orphan", "error"])
    sp_log.add_argument("--table", choices=["consoles", "pdus"])
    sp_log.add_argument("--limit", type=int, default=50)
    sp_log.set_defaults(func=cmd_log)

    sp_mark = sub.add_parser(
        "mark-manual",
        help="Mark a row as manually maintained so future syncs never overwrite it",
    )
    sp_mark.add_argument("what", choices=["consoles", "pdus"])
    sp_mark.add_argument("device")
    sp_mark.add_argument("--pdu", help="Only for `pdus`: limit to this PDU name")
    sp_mark.add_argument("--note", help="Optional note saved with the row")
    sp_mark.set_defaults(func=cmd_mark_manual)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
