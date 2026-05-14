#!/usr/bin/env python3
"""
console — connect to a DriveNets device via its console server
Usage:
  console [user@]SERIAL
  console --check [user@]SERIAL     — show console + PDU info, no connect
  console --fix [user@]SERIAL
  console -r [user@]SERIAL          — power off PDU outlet 5s then on, then connect
  console -r --down-only [user@]SERIAL  — power off only, show PDU CLI status, exit
  console -r --power-on-only [user@]SERIAL  — power on only, then connect
  console -r --fix [user@]SERIAL   — fix wrong PDU mapping, power cycle, connect
"""
import sys, os, subprocess, csv, re, signal, json, socket, warnings, shutil, stat
warnings.filterwarnings("ignore")
import logging
logging.captureWarnings(True)
from concurrent.futures import ThreadPoolExecutor, as_completed


def _load_env_file(path="~/.console_env"):
    """Source a shell-style 'export KEY=value' file into os.environ.

    Lets the tool work over non-interactive SSH where ~/.bashrc is not loaded.
    Existing environment values always win (we use setdefault), so a real
    `export` in the user's shell still beats whatever is in this file.
    """
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
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)
    except (OSError, FileNotFoundError):
        pass


_load_env_file()

# Remote merge script that refreshes the local Device42 cache and merges
# any NEW console + PDU rows into console_devices.csv / pdu_mapping.json.
# Never overwrites existing entries; mismatches are only logged.
D42_MERGE_REMOTE = "/home/dn/console_db/d42_merge.py"
D42_MERGE_LOG    = "/home/dn/console_db/d42_merge.log"

# ---------------------------------------------------------------------------
# Version + "What's new" banner.
#
# Architecture note: the Mac wrapper (~/bin/console) SSHes to zkeiserman-dev
# and runs *this* script there. There is no separate Mac copy to update --
# every Mac invocation already executes the latest console.py on the dev VM.
# So instead of a download-and-install update flow, we keep a per-user
# "last seen version" file (~/.console_last_seen) and show the latest
# CHANGELOG entry whenever __version__ has changed since the previous run.
#
# Set CONSOLE_SKIP_UPDATE_CHECK=1 to suppress the banner entirely.
# ---------------------------------------------------------------------------
__version__ = "2026.05.14.1"
CHANGELOG_PATH    = "/home/dn/console_db/CHANGELOG.md"
LAST_SEEN_PATH    = os.path.expanduser("~/.console_last_seen")
UPDATE_SKIP_ENV   = "CONSOLE_SKIP_UPDATE_CHECK"


def _read_last_seen():
    try:
        with open(LAST_SEEN_PATH) as f:
            return f.read().strip()
    except (FileNotFoundError, OSError):
        return ""


def _write_last_seen(version):
    try:
        with open(LAST_SEEN_PATH, "w") as f:
            f.write(version)
    except OSError:
        pass


def _latest_changelog_section():
    """Return the top section of CHANGELOG.md (everything from the first
    'vX.Y.Z' header until the next blank line + next header)."""
    try:
        with open(CHANGELOG_PATH) as f:
            lines = f.read().splitlines()
    except (FileNotFoundError, OSError):
        return []
    section = []
    started = False
    header_re = re.compile(r'^v\d')
    for line in lines:
        if header_re.match(line):
            if started:
                break
            started = True
        if started:
            section.append(line.rstrip())
    # trim trailing blanks
    while section and not section[-1].strip():
        section.pop()
    return section


def _show_version_banner():
    """Always print the current version. If __version__ has changed since the
    user's previous run, also print the latest CHANGELOG entry."""
    if os.environ.get(UPDATE_SKIP_ENV) == "1":
        return

    last_seen = _read_last_seen()
    if last_seen == __version__:
        return  # already on this version -- stay silent

    bar = "=" * 64
    print()
    print(bar)
    if last_seen:
        print(f"  console UPDATED:  v{last_seen}  ->  v{__version__}")
    else:
        print(f"  console v{__version__}  (first run on this account)")
    print(bar)

    section = _latest_changelog_section()
    if section:
        print("  What's new:")
        for line in section:
            print(f"    {line}")
    print(bar)
    print(f"  (silence this banner with: export {UPDATE_SKIP_ENV}=1)")
    print()

    _write_last_seen(__version__)

try:
    import paramiko
except ImportError:
    paramiko = None

# All credentials and host names come from environment variables so this
# file can live in a public repo. See README.md for the full list and
# /home/dn/.console_env for an example bashrc snippet.
DB_SERVER      = os.environ.get("DN_SERVER_HOST", "")
DB_USER        = os.environ.get("DN_SERVER_USER", "dn")
DB_PASS        = os.environ.get("DN_SERVER_PASSWORD", "")
DB_REMOTE      = os.environ.get("CONSOLE_CSV_PATH", "/home/dn/console_db/console_devices.csv")
LOCAL_CSV      = os.environ.get("CONSOLE_CSV_CACHE", "/tmp/console_devices_cache.csv")
PDU_MAP_REMOTE = os.environ.get("PDU_MAP_PATH", "/home/dn/console_db/pdu_mapping.json")
LOCAL_PDU_MAP  = os.environ.get("PDU_MAP_CACHE", "/tmp/pdu_mapping_cache.json")
PDU_CLI_CONFIG = os.environ.get("PDU_CLI_CONFIG_PATH", "/home/dn/console_db/pdu_cli_config.json")
PDU_USER       = os.environ.get("CONSOLE_PDU_USER", "dn")
PDU_PASS       = os.environ.get("CONSOLE_PDU_PASSWORD", "")
PDU_PASS_ALT   = os.environ.get("CONSOLE_PDU_PASSWORD_ALT", "")
CS_USER        = os.environ.get("CONSOLE_CS_USER", "dn")
CS_PASS        = os.environ.get("CONSOLE_CS_PASSWORD", "")


# ── DB sync helpers ────────────────────────────────────────────────────────────

def _scp_from(remote, local):
    cmd = f"scp -o StrictHostKeyChecking=no {DB_USER}@{DB_SERVER}:{remote} {local}"
    subprocess.run(cmd, shell=True, capture_output=True)

def _scp_to(local, remote):
    cmd = f"scp -o StrictHostKeyChecking=no {local} {DB_USER}@{DB_SERVER}:{remote}"
    subprocess.run(cmd, shell=True, capture_output=True)

def fetch_db():
    _scp_from(DB_REMOTE, LOCAL_CSV)

def fetch_pdu_map():
    _scp_from(PDU_MAP_REMOTE, LOCAL_PDU_MAP)


def trigger_d42_merge_async():
    """Fire-and-forget: refresh Device42 cache and merge new entries on the
    dev VM. Returns immediately; the console connect does not wait for it.
    The merge script throttles itself (won't run more than once per 5 min)
    and locks against concurrent invocations, so spamming `console` is safe.
    Any error is silently swallowed -- this must never block the user.
    """
    remote_cmd = (
        f"nohup python3 {D42_MERGE_REMOTE} "
        f">> {D42_MERGE_LOG} 2>&1 < /dev/null &"
    )
    try:
        # If we happen to be running on the dev VM itself, skip SSH.
        on_dev_vm = socket.gethostname().lower().startswith(
            DB_SERVER.split('.')[0].lower()
        )
        if on_dev_vm:
            subprocess.Popen(
                ["bash", "-c", remote_cmd],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        else:
            subprocess.Popen(
                ["ssh", "-o", "StrictHostKeyChecking=no",
                 "-o", "ConnectTimeout=5",
                 f"{DB_USER}@{DB_SERVER}", remote_cmd],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
    except Exception as e:
        print(f"[d42] could not trigger background sync: {e}", flush=True)

def read_db():
    if not os.path.exists(LOCAL_CSV):
        return []
    rows = []
    with open(LOCAL_CSV, newline='') as f:
        for row in csv.reader(f):
            if row:
                rows.append(row)
    return rows

def write_db(rows):
    with open(LOCAL_CSV, 'w', newline='') as f:
        csv.writer(f).writerows(rows)
    _scp_to(LOCAL_CSV, DB_REMOTE)

def read_pdu_map():
    if not os.path.exists(LOCAL_PDU_MAP):
        return {}
    with open(LOCAL_PDU_MAP) as f:
        return json.load(f)

def write_pdu_map(m):
    with open(LOCAL_PDU_MAP, 'w') as f:
        json.dump(m, f, indent=2)
    _scp_to(LOCAL_PDU_MAP, PDU_MAP_REMOTE)


# ── Lookup helpers ─────────────────────────────────────────────────────────────

def normalize_console(s):
    s = s.strip().upper()
    if s and not s.startswith("CONSOLE-"):
        s = "CONSOLE-" + s
    return s

def normalize_pdu(s):
    s = s.strip().lower()
    if s and not s.startswith("pdu-"):
        s = "pdu-" + s
    return s

def lookup(serial_upper, rows):
    for row in rows:
        if len(row) >= 3 and row[2].strip().upper() == serial_upper:
            return row[0].strip(), row[1].strip()
    return None, None

def pdu_lookup(serial_upper):
    m = read_pdu_map()
    entry = m.get(serial_upper)
    if not entry:
        return []
    if isinstance(entry, list):
        return entry
    return [entry]

def prompt(msg):
    try:
        return input(msg)
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)

def ticket(action, serial, console=None, port=None):
    print()
    print("\u2501" * 56)
    print(" Please open a Jira ticket to IT:")
    print(f"   Summary : [{action}] {serial}")
    if console and port:
        print(f"   Details : Connect {serial} to {console} port {port}")
    print("   Project : IT / Lab Infrastructure")
    print("\u2501" * 56)
    print()


# ── PDU helpers ────────────────────────────────────────────────────────────────

def _pdu_run_cmd(shell, cmd, wait=2.0):
    import time
    shell.send(cmd + "\n")
    time.sleep(wait)
    out = ""
    while shell.recv_ready():
        out += shell.recv(65536).decode("utf-8", errors="replace")
    return out

def _pdu_cli_type(pdu_host):
    h = normalize_pdu(pdu_host)
    try:
        with open(PDU_CLI_CONFIG) as f:
            cfg = json.load(f)
        for mode, hosts in cfg.items():
            if h in [x.lower() for x in hosts]:
                return mode
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return "dev_outlet"

def _pdu_off(shell, pdu_host, outlet, cli_type):
    if cli_type == "dev_outlet":
        return _pdu_run_cmd(shell, f"dev outlet 1 {outlet} off", wait=2.0)
    return _pdu_run_cmd(shell, f"olOff {outlet}", wait=2.0)

def _pdu_on(shell, pdu_host, outlet, cli_type):
    if cli_type == "dev_outlet":
        return _pdu_run_cmd(shell, f"dev outlet 1 {outlet} on", wait=2.0)
    return _pdu_run_cmd(shell, f"olOn {outlet}", wait=2.0)

def _pdu_status(shell, pdu_host, outlet, cli_type):
    if cli_type == "dev_outlet":
        return _pdu_run_cmd(shell, f"dev outlet 1 {outlet} status", wait=2.0)
    return _pdu_run_cmd(shell, f"olStatus {outlet}", wait=2.0)

def _pdu_is_off(txt, cli_type):
    txt = txt.lower()
    if cli_type == "dev_outlet":
        return "close" in txt
    return re.search(r"\boff\b", txt) is not None

def _pdu_is_on(txt, cli_type):
    txt = txt.lower()
    if cli_type == "dev_outlet":
        return "open" in txt
    return re.search(r"\bon\b", txt) is not None


def _pdu_connect(pdu_host, password):
    import time
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(pdu_host, username=PDU_USER, password=password, timeout=15,
                   look_for_keys=False, allow_agent=False)
    shell = client.invoke_shell()
    time.sleep(2)
    if shell.recv_ready():
        shell.recv(10000)
    return client, shell


def _pdu_power_off_only_paramiko(pdu_host, outlet, password=None):
    import time
    if not paramiko:
        print("Install paramiko: pip install paramiko")
        sys.exit(1)
    cli_type = _pdu_cli_type(pdu_host)
    pwd = password or PDU_PASS
    client, shell = _pdu_connect(pdu_host, pwd)
    transport = client.get_transport()
    _pdu_off(shell, pdu_host, outlet, cli_type)
    time.sleep(2)
    status_txt = _pdu_status(shell, pdu_host, outlet, cli_type)
    if _pdu_is_off(status_txt, cli_type):
        print(f"  {pdu_host} outlet {outlet}: OFF", flush=True)
    else:
        print(f"  {pdu_host} outlet {outlet}: unexpected status — check PDU", flush=True)
    transport.close()


def _pdu_power_on_only_paramiko(pdu_host, outlet, password=None):
    import time
    if not paramiko:
        print("Install paramiko: pip install paramiko")
        sys.exit(1)
    cli_type = _pdu_cli_type(pdu_host)
    pwd = password or PDU_PASS
    client, shell = _pdu_connect(pdu_host, pwd)
    transport = client.get_transport()
    _pdu_on(shell, pdu_host, outlet, cli_type)
    time.sleep(2)
    status_txt = _pdu_status(shell, pdu_host, outlet, cli_type)
    if _pdu_is_on(status_txt, cli_type):
        print(f"  {pdu_host} outlet {outlet}: ON", flush=True)
    else:
        print(f"  {pdu_host} outlet {outlet}: unexpected status — check PDU", flush=True)
    transport.close()


def _pdu_reboot_paramiko(pdu_host, outlet, password=None):
    import time
    if not paramiko:
        print("Install paramiko for PDU reboot: pip install paramiko")
        sys.exit(1)
    cli_type = _pdu_cli_type(pdu_host)
    pwd = password or PDU_PASS
    client, shell = _pdu_connect(pdu_host, pwd)
    transport = client.get_transport()

    _pdu_off(shell, pdu_host, outlet, cli_type)
    time.sleep(2)
    status_txt = _pdu_status(shell, pdu_host, outlet, cli_type)
    if _pdu_is_off(status_txt, cli_type):
        print(f"  {pdu_host} outlet {outlet}: OFF verified ({'Close' if cli_type == 'dev_outlet' else 'off'})", flush=True)
    else:
        print(f"  {pdu_host} outlet {outlet}: status: {status_txt.strip()!r}", flush=True)

    time.sleep(3)

    _pdu_on(shell, pdu_host, outlet, cli_type)
    time.sleep(2)
    status_txt = _pdu_status(shell, pdu_host, outlet, cli_type)
    if _pdu_is_on(status_txt, cli_type):
        print(f"  {pdu_host} outlet {outlet}: ON verified ({'Open' if cli_type == 'dev_outlet' else 'on'})", flush=True)
    else:
        print(f"  {pdu_host} outlet {outlet}: status: {status_txt.strip()!r}", flush=True)

    transport.close()


def pdu_power_off_only(pdu_host, outlet):
    last_err = None
    for pwd in (PDU_PASS, PDU_PASS_ALT):
        try:
            _pdu_power_off_only_paramiko(pdu_host, outlet, pwd)
            return
        except paramiko.ssh_exception.AuthenticationException as e:
            last_err = e
            continue
        except Exception:
            raise
    raise last_err


def pdu_power_on_only(pdu_host, outlet):
    last_err = None
    for pwd in (PDU_PASS, PDU_PASS_ALT):
        try:
            _pdu_power_on_only_paramiko(pdu_host, outlet, pwd)
            return
        except paramiko.ssh_exception.AuthenticationException as e:
            last_err = e
            continue
        except Exception:
            raise
    raise last_err


def pdu_reboot_outlet(pdu_host, outlet):
    last_err = None
    for pwd in (PDU_PASS, PDU_PASS_ALT):
        try:
            _pdu_reboot_paramiko(pdu_host, outlet, pwd)
            return
        except paramiko.ssh_exception.AuthenticationException as e:
            last_err = e
            continue
        except Exception:
            raise
    raise last_err


# ── Console connect ────────────────────────────────────────────────────────────

def _drain_chan(chan, secs):
    """Read everything the channel emits over the next `secs` seconds."""
    import time
    out = ""
    end = time.time() + secs
    while time.time() < end:
        if chan.recv_ready():
            out += chan.recv(16384).decode("utf-8", errors="replace")
        else:
            time.sleep(0.05)
    return out


def _report_busy_and_exit(chan, console_server, port_num, serial):
    """Port is held in EXCLUSIVE mode by another session.

    Walk the SN9116CO admin menus to gather (a) the serial-port table (which
    confirms which ports are Busy) and (b) the session list (so the user can
    see who else is on the chassis), then print a clean summary and exit
    cleanly so the user doesn't get stranded in the Press-Enter loop.
    """
    import socket
    import time

    sessions_raw = ""
    serial_raw = ""
    try:
        chan.send("\r")           # ack the "Press Enter to continue" prompt
        time.sleep(0.3)
        chan.send("Q\r")          # back to Port Access list
        time.sleep(0.3)
        chan.send("Q\r")          # back to Main Menu
        time.sleep(0.5)
        _drain_chan(chan, 0.5)
        chan.send("7\r")          # 7 = CLI Mode
        time.sleep(0.8)
        _drain_chan(chan, 0.8)
        chan.send("serial\r")
        serial_raw = _drain_chan(chan, 1.8)
        chan.send("session\r")
        sessions_raw = _drain_chan(chan, 1.8)
        chan.send("quit\r")
    except Exception:
        # Best-effort: even if menu navigation breaks, we still want to print
        # what we know.
        pass

    # Parse session table - SN9116CO format we observed:
    #   "1   | dn   | SSH | 10.10.73.232 | 0  | 2026-05-05 09:24:52 | ..."
    # Skip the dev VM we tunnelled through (this very process); the user is
    # never the one holding the port if they're getting "busy".
    self_ips = set()
    try:
        self_ips.add(socket.gethostbyname(socket.gethostname()))
    except Exception:
        pass
    self_ips.add("127.0.0.1")

    others = []
    for line in (sessions_raw or "").splitlines():
        m = re.match(
            r"\s*(\d+)\s*\|\s*(\S+)\s*\|\s*(\S+)\s*\|\s*(\d{1,3}(?:\.\d{1,3}){3})"
            r"\s*\|\s*(\d+)\s*\|\s*([\d\-]+\s+[\d:]+)",
            line,
        )
        if not m:
            continue
        sid, who, svc, ip, port, login = m.groups()
        if ip in self_ips:
            continue
        host = ""
        try:
            host = socket.gethostbyaddr(ip)[0]
        except Exception:
            pass
        others.append((sid, who, ip, host, login))

    print()
    print("=" * 72)
    print(f"  PORT BUSY: {serial}  ({console_server} port {port_num})")
    print("=" * 72)
    print("  The console-server reported:")
    print("     'Exclusive mode and port busy! Press Enter to continue...'")
    print("  Another user has the serial port locked in exclusive mode.")
    print()
    if others:
        print("  Other active sessions on the chassis:")
        for sid, who, ip, host, login in others:
            label = host or ip
            print(f"     [{sid}] {who:<8}  from {label:<40}  since {login}")
        print()
        print("  Ping the most likely owner (oldest session) to release the port,")
        print("  or escalate via Slack/Jira before forcibly killing their session")
        print(f"  from CONSOLE-{console_server.split('-')[-1]} -> Sessions menu.")
    else:
        print("  Could not enumerate active sessions on the chassis.")
        print("  Login to the chassis manually and check the Sessions menu.")
    print("=" * 72)
    sys.exit(2)


def connect(serial, console_server, port_num):
    if not paramiko:
        print("Install paramiko for console connect: pip install paramiko")
        sys.exit(1)
    import select
    import tty
    import termios
    import time
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(console_server, username=CS_USER, password=CS_PASS,
                       timeout=15, look_for_keys=False, allow_agent=False)
    except Exception as e:
        print(f"Cannot connect to {console_server}: {e}")
        sys.exit(1)
    chan = client.invoke_shell(term="xterm", width=80, height=24)
    chan.settimeout(0.0)
    buf = ""
    for _ in range(50):
        if chan.recv_ready():
            buf += chan.recv(4096).decode("utf-8", errors="replace")
        if "Select one:" in buf or "Main Menu" in buf:
            break
        time.sleep(0.1)
    if "Select one:" not in buf and "Main Menu" not in buf:
        print("ERROR: no menu (got:", buf[:200], ")")
        sys.exit(1)

    # Parse the main menu to find which slot is "Port Access".
    # Different console-server models put it at different numbers
    # (e.g. WB consoles -> 3, SN9116CO -> 4). Hardcoding 3 lands you in
    # "Port Settings" on a SN9116CO, which silently corrupts the next
    # keystrokes. Match  "  N. Port Access"  with whitespace tolerance.
    port_access_match = re.search(
        r"(?im)^\s*(\d+)\s*\.\s*Port\s+Access\b",
        buf,
    )
    port_access_key = port_access_match.group(1) if port_access_match else "3"

    chan.send(port_access_key + "\r")
    time.sleep(0.3)
    buf = ""
    for _ in range(30):
        if chan.recv_ready():
            buf += chan.recv(4096).decode("utf-8", errors="replace")
        if "Port Access" in buf or "port" in buf.lower():
            break
        time.sleep(0.1)
    chan.send(str(port_num) + "\r")
    time.sleep(0.3)
    chan.send("\r")
    time.sleep(0.3)

    # Sniff the post-select buffer for ~1.5s. If the SN9116CO came back with
    # "Exclusive mode and port busy", another user holds the port - bail out
    # cleanly with a useful summary instead of dropping the user into the
    # menu's "Press Enter to continue..." trap with no clue who's hogging it.
    post = ""
    busy_deadline = time.time() + 1.5
    while time.time() < busy_deadline:
        if chan.recv_ready():
            post += chan.recv(8192).decode("utf-8", errors="replace")
        else:
            time.sleep(0.05)
    if re.search(r"Exclusive mode and port busy|port\s+busy|in\s+use", post, re.I):
        _report_busy_and_exit(chan, console_server, port_num, serial)
        return  # _report_busy_and_exit() always exits, but keep linters happy

    filter_patterns = [
        r"Press \[Ctrl\+? ?d\] to go to the Suspend Menu\.?\r?\n?",
        rf"(?:^|\r?\n){re.escape(str(port_num))}\.?\s*[^\n]*[\r\n]+",
        r"(?:^|\r?\n)Connected to Port: \d+[\r\n]+",
    ]
    filter_re = re.compile("|".join(f"({p})" for p in filter_patterns), re.I)
    oldtty = termios.tcgetattr(sys.stdin)
    try:
        tty.setraw(sys.stdin.fileno())
        tty.setcbreak(sys.stdin.fileno())
        while True:
            r, _, _ = select.select([chan, sys.stdin], [], [])
            if chan in r:
                data = chan.recv(1024)
                if not data:
                    break
                text = data.decode("utf-8", errors="replace")
                text = filter_re.sub("", text)
                if text:
                    sys.stdout.write(text)
                    sys.stdout.flush()
            if sys.stdin in r:
                x = sys.stdin.read(1)
                if not x:
                    break
                chan.send(x)
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    _show_version_banner()

    fix_mode = False
    reboot_mode = False
    down_only = False
    power_on_only = False
    check_mode = False
    args = sys.argv[1:]

    while args and args[0] in ("--fix", "-r", "--down-only", "--power-on-only", "--check"):
        if args[0] == "--fix":
            fix_mode = True
        elif args[0] == "--check":
            check_mode = True
        elif args[0] == "--down-only":
            down_only = True
            reboot_mode = True
        elif args[0] == "--power-on-only":
            power_on_only = True
            reboot_mode = True
        else:
            reboot_mode = True
        args = args[1:]

    if not args:
        print("Usage:  console [user@]SERIAL")
        print("        console --check [user@]SERIAL     — show console + PDU info, no connect")
        print("        console --fix [user@]SERIAL")
        print("        console -r [user@]SERIAL          — power off PDU 5s then on, then connect")
        print("        console -r --down-only [user@]SERIAL  — power off only, show status, exit")
        print("        console -r --power-on-only [user@]SERIAL  — power on only, then connect")
        print("        console -r --fix [user@]SERIAL   — fix PDU mapping, power cycle, connect")
        sys.exit(1)

    arg = args[0]
    serial = arg.split("@")[-1]
    serial_upper = serial.upper()

    print("", flush=True)
    trigger_d42_merge_async()
    fetch_db()
    fetch_pdu_map()
    rows = read_db()

    # ── Check mode ────────────────────────────────────────────────────────────
    if check_mode:
        console, port = lookup(serial_upper, rows)
        print(f"Console:  ", end="")
        if console:
            print(f"{serial_upper}  \u2192  {console}  port {port}")
        else:
            print(f"NOT FOUND in database")
        pdu_ent = pdu_lookup(serial_upper)
        print(f"PDU:      ", end="")
        if pdu_ent:
            for e in pdu_ent:
                print(f"{serial_upper}  \u2192  {normalize_pdu(e['pdu'])}  outlet {e['outlet']}")
        else:
            print(f"NOT FOUND in database")
        sys.exit(0)

    # ── PDU reboot (before connect) ───────────────────────────────────────────
    if reboot_mode and not check_mode:
        pdu_entries = pdu_lookup(serial_upper)
        if fix_mode or not pdu_entries:
            if pdu_entries:
                print(f"Current PDU(s) for {serial_upper}:")
                for i, e in enumerate(pdu_entries, 1):
                    print(f"  {i}. {e['pdu']} outlet {e['outlet']}")
            else:
                print(f"No PDU mapping for '{serial}' — add one.\n")
            n_str = prompt("How many PDUs? (e.g. 1 or 2 for dual-PSU): ").strip()
            if not n_str:
                print("Aborted.")
                sys.exit(0)
            try:
                n_pdus = int(n_str)
            except ValueError:
                print("Invalid number.")
                sys.exit(1)
            if n_pdus < 1:
                print("Aborted.")
                sys.exit(0)
            entries = []
            for i in range(1, n_pdus + 1):
                print(f"\n  PDU {i}:")
                while True:
                    pdu_input = normalize_pdu(prompt("    PDU host (e.g. pdu-b10-1 or b10-1) [pdu-b10-1]: ").strip() or "pdu-b10-1")
                    if pdu_input:
                        break
                while True:
                    outlet_input = prompt("    Outlet number: ").strip()
                    if outlet_input:
                        try:
                            outlet_num = int(outlet_input)
                            break
                        except ValueError:
                            print("    Invalid — enter a number.")
                    else:
                        print("    Required.")
                entries.append({"pdu": pdu_input, "outlet": outlet_num})
            m = read_pdu_map()
            m[serial_upper] = entries if len(entries) > 1 else entries[0]
            write_pdu_map(m)
            pdu_entries = entries
            print(f"\nSaved to DB: {serial_upper} \u2192 {len(entries)} PDU(s)")
            if fix_mode:
                sys.exit(0)

        to_cycle = pdu_entries[:1]
        pdu_list = ", ".join(f"{normalize_pdu(e['pdu'])} outlet {e['outlet']}" for e in to_cycle)

        if down_only:
            print(f"Power OFF only (no power-on): {pdu_list}", flush=True)
            e = to_cycle[0]
            pdu_host = normalize_pdu(e["pdu"])
            try:
                pdu_power_off_only(pdu_host, e["outlet"])
            except Exception as ex:
                print(f"  ERROR: {ex}", flush=True)
                sys.exit(1)
            print("\nDone. Outlet is OFF.", flush=True)
            sys.exit(0)

        elif power_on_only:
            print(f"Power ON only: {pdu_list}", flush=True)
            e = to_cycle[0]
            pdu_host = normalize_pdu(e["pdu"])
            try:
                pdu_power_on_only(pdu_host, e["outlet"])
            except Exception as ex:
                print(f"  ERROR: {ex}", flush=True)
                sys.exit(1)

        if not power_on_only:
            print(f"Power off 5s then on: {pdu_list}", flush=True)
            def _reboot_one(e):
                pdu_host = normalize_pdu(e["pdu"])
                try:
                    pdu_reboot_outlet(pdu_host, e["outlet"])
                    return None
                except Exception as ex:
                    return (pdu_host, str(ex))
            with ThreadPoolExecutor(max_workers=len(to_cycle)) as ex:
                futures = {ex.submit(_reboot_one, e): e for e in to_cycle}
                for fut in as_completed(futures):
                    err = fut.result()
                    if err:
                        pdu_host, msg = err
                        print(f"  WARNING: PDU power cycle failed ({pdu_host}): {msg}", flush=True)
                        print(f"  Continuing anyway — fix with: console -r --fix {serial}", flush=True)
        import time
        time.sleep(8)
        print("", flush=True)

    # ── Fix mode (console mapping only, when not -r) ──────────────────────────
    if fix_mode and not reboot_mode:
        console, port = lookup(serial_upper, rows)
        if console:
            print(f"Current entry: {serial_upper} \u2192 {console} port {port}")
        cs_input = normalize_console(prompt(f"New console server for {serial_upper} (e.g. B02): "))
        new_port  = prompt("New port number: ")
        if not cs_input or not new_port:
            print("Aborted.")
            sys.exit(0)
        rows = [r for r in rows if not (len(r) >= 3 and r[2].strip().upper() == serial_upper)]
        rows.append([cs_input, new_port, serial_upper])
        write_db(rows)
        print(f"\nFixed: {serial_upper} \u2192 {cs_input} port {new_port}  (synced to {DB_SERVER})")
        ticket("Fix console port", serial_upper, cs_input, new_port)
        sys.exit(0)

    # ── Normal connect ─────────────────────────────────────────────────────────
    console, port = lookup(serial_upper, rows)

    if not console:
        print(f"Device '{serial}' not found — adding new entry.\n")
        cs_input = normalize_console(prompt("Console server (e.g. B02 or CONSOLE-B02): "))
        port      = prompt("Port number (e.g. 5): ")
        if not cs_input or not port:
            print("Aborted.")
            sys.exit(0)
        console = cs_input
        rows.append([console, port, serial_upper])
        write_db(rows)
        print(f"\nAdded: {serial_upper} \u2192 {console} port {port}  (synced to {DB_SERVER})")
        ticket("Add console", serial_upper, console, port)

    print(f"Found: {serial}  \u2192  {console}  port {port}", flush=True)
    pdu_ent = pdu_lookup(serial_upper)
    if reboot_mode and pdu_ent:
        print(f"  (To fix wrong PDU: console -r --fix {serial})", flush=True)
    print(">>> To exit console type ctrl+D", flush=True)
    connect(serial, console, port)


if __name__ == "__main__":
    main()
