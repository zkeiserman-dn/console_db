# console_db

A small toolkit for connecting to DriveNets lab devices over their console
servers, with optional PDU power control and an automatic Device42 sync
that keeps the local console + PDU mappings up to date.

The tool is split into a thin Mac-side wrapper that just SSHes to a dev VM,
plus three Python scripts that run on that dev VM:

| File | Where it runs | What it does |
|---|---|---|
| `console_mac` + `console_expect` | Your Mac | 5-line bash + expect wrapper. Reads `DN_SERVER_HOST` / `DN_SERVER_PASSWORD` and SSHes to the dev VM. |
| `console.py` | Dev VM | The real tool. Looks up the requested device's console-server + port in `console_devices.csv`, optionally power-cycles the device via PDU, then drops you into the serial console session. |
| `dump_d42_consoles.py` | Dev VM | Pulls every console + PDU mapping from Device42 (DOQL API) into a local SQLite cache. Idempotent; "add new, never overwrite". |
| `d42_merge.py` | Dev VM | Runs `dump_d42_consoles.py`, then merges any new rows from the SQLite cache into `console_devices.csv` / `pdu_mapping.json`. Triggered automatically by `console.py` on every invocation (throttled to once per 5 min). |

## Quick start

### 1. On the dev VM

```bash
git clone https://github.com/zkeiserman-dn/console_db.git ~/console_db
cd ~/console_db
pip install --user -r requirements.txt    # if you don't have requests/paramiko/waiting yet

# Create a private env file with credentials (NEVER commit this).
cp .console_env.example ~/.console_env       # if you have an example, otherwise:
cat > ~/.console_env <<'EOF'
export DN_SERVER_HOST="your-dev-vm"
export DN_SERVER_USER="dn"
export DN_SERVER_PASSWORD="xxx"

export CONSOLE_CS_USER="dn"
export CONSOLE_CS_PASSWORD="xxx"

export CONSOLE_PDU_USER="dn"
export CONSOLE_PDU_PASSWORD="xxx"
export CONSOLE_PDU_PASSWORD_ALT="xxx"

export DEVICE42_ENDPOINT="https://your-device42/services/data/v1.0/query/"
export DEVICE42_AUTH="Basic $(printf '%s' user:pass | base64)"
EOF
chmod 600 ~/.console_env
```

### 2. On your Mac

```bash
# 1. Set the variables the wrapper needs to reach the dev VM:
cat >> ~/.zshrc <<'EOF'
export DN_SERVER_HOST="your-dev-vm"
export DN_SERVER_PASSWORD="xxx"
EOF
source ~/.zshrc

# 2. Install the two tiny wrappers from the repo:
mkdir -p ~/bin
scp dn@$DN_SERVER_HOST:~/console_db/console_mac    ~/bin/console
scp dn@$DN_SERVER_HOST:~/console_db/console_expect ~/bin/console_expect
chmod +x ~/bin/console ~/bin/console_expect
```

### 3. Use it

```bash
console <SERIAL>                 # connect (looks up console + PDU info)
console --check <SERIAL>         # show console + PDU info, do not connect
console -r <SERIAL>              # power-cycle the PDU outlet, then connect
console -r --down-only <SERIAL>  # power-off only
console -r --power-on-only <SERIAL>
console --fix <SERIAL>           # fix the console-server mapping (interactive)
console -r --fix <SERIAL>        # fix the PDU mapping
```

The first time `console.py` sees a new `__version__` it prints a one-time
"What's new" banner with the latest CHANGELOG entry. Subsequent runs are
silent. To suppress the banner permanently:

```bash
export CONSOLE_SKIP_UPDATE_CHECK=1
```

## Configuration

All credentials and host names come from environment variables — nothing is
hardcoded. Anything you don't set has an empty default, so the script will
fail loudly with the name of the missing variable rather than try to use a
secret that isn't there.

### Mac-side (read by `console_expect`)

| Variable | Required? | Purpose |
|---|---|---|
| `DN_SERVER_HOST` | yes | Hostname of the dev VM you SSH into |
| `DN_SERVER_PASSWORD` | yes | SSH password for that host |
| `DN_SERVER_USER` | no (default `dn`) | SSH username |

### Dev-VM-side (read by `console.py` + the merge scripts)

| Variable | Default | Purpose |
|---|---|---|
| `CONSOLE_CS_USER` | `dn` | Console-server SSH username |
| `CONSOLE_CS_PASSWORD` | (empty) | Console-server SSH password |
| `CONSOLE_PDU_USER` | `dn` | PDU SSH username |
| `CONSOLE_PDU_PASSWORD` | (empty) | PDU SSH password |
| `CONSOLE_PDU_PASSWORD_ALT` | (empty) | Alternate PDU password (older firmware) |
| `DEVICE42_ENDPOINT` | example URL | DOQL endpoint, e.g. `https://device42/services/data/v1.0/query/` |
| `DEVICE42_AUTH` | (empty) | Full HTTP `Authorization` header, e.g. `Basic <base64(user:pass)>` |
| `CONSOLE_CSV_PATH` | `/home/dn/console_db/console_devices.csv` | Where the local CSV lives |
| `PDU_MAP_PATH` | `/home/dn/console_db/pdu_mapping.json` | Where the local PDU mapping lives |
| `PDU_CLI_CONFIG_PATH` | `/home/dn/console_db/pdu_cli_config.json` | Per-PDU CLI flavor |
| `DEVICE42_CACHE_PATH` | `~/device42_cache.sqlite` | SQLite cache for D42 sync |
| `CONSOLE_DB_DIR` | dir of `d42_merge.py` | Override if you cloned somewhere else |
| `CONSOLE_SKIP_UPDATE_CHECK` | unset | Set to `1` to suppress the "what's new" banner |

The recommended way is to put the secret values in `~/.console_env` (mode `600`)
on the dev VM. `console_expect` automatically `source`s that file over SSH
before invoking `console.py`, so the values land in the script's environment
without going through `~/.bashrc`.

## How the Device42 sync works

Every `console <SERIAL>` run fires a fire-and-forget background process on
the dev VM:

1. `d42_merge.py` acquires a non-blocking file lock so simultaneous `console`
   invocations don't collide. If the lock is held, the new run exits silently.
2. If the previous successful merge ran < 5 minutes ago, this run exits
   silently as well (throttle). Override with `--force`.
3. `dump_d42_consoles.py sync` is invoked. It runs two DOQL queries against
   Device42 (one for console-server connections, one for PDU outlets +
   models) and writes them into a local SQLite cache.
4. `d42_merge.py` then iterates over the cache and merges into the on-disk
   files:
   - **Row in D42, not in local file** -> insert.
   - **Row in D42, identical to local** -> nothing.
   - **Row in D42, different from local** -> *left as-is*. The disagreement
     is written to `d42_merge.log` for human review. The assumption is
     that any change you made locally is a deliberate fix, and the tool
     must never silently revert it.
   - **Row no longer in D42** -> kept (logged as `orphan`).
5. Disagreements between Device42 and the local files are findable with:

```bash
grep "mismatch" /home/dn/console_db/d42_merge.log
grep "needs_review" /home/dn/console_db/d42_merge.log
```

## Versioning + the "what's new" banner

`console.py` carries a single `__version__` string. The first time you
run it after that string changes (compared to `~/.console_last_seen`), it
prints a banner:

```
================================================================
  console UPDATED:  v2026.04.27.4  ->  v2026.04.27.5
================================================================
  What's new:
    v2026.04.27.5  (2026-04-27)
      * <bullet>
================================================================
```

Subsequent runs are silent until the next bump. Release workflow:

1. Edit `console.py`, bump `__version__`.
2. Prepend a section to `CHANGELOG.md`.
3. `git commit && git push`. (On the dev VM, just save the file in place if
   that's where you maintain the master.)

## Files

```
console_db/
├── console_mac           # Mac-side bash launcher (~/bin/console)
├── console_expect        # Mac-side expect wrapper that SSHes to the dev VM
├── console.py            # main tool (runs on the dev VM)
├── d42_merge.py          # CSV/JSON merge orchestrator (runs on the dev VM)
├── dump_d42_consoles.py  # Device42 DOQL -> SQLite sync
├── pdu_cli_config.json   # PDU CLI flavor per host
├── CHANGELOG.md          # version history
├── LICENSE               # MIT
└── README.md             # this file
```

The following live in the same directory at runtime but are **not** committed
(see `.gitignore`):

```
console_devices.csv      # your lab's console-server <-> serial table
pdu_mapping.json         # your lab's serial -> PDU outlet table
device42_cache.sqlite    # D42 mirror, regenerated on demand
d42_merge.log            # rotating log of every merge
.d42_merge.last          # throttle stamp
.d42_merge.lock          # cross-process lock
.console_last_seen       # per-user last-seen __version__
.console_env             # private secrets file (mode 600)
```

## Safety nets

- **Secrets only in env vars.** The repo contains no passwords, no tokens,
  no internal hostnames. Misconfigured machines fail loudly instead of
  silently using a baked-in default.
- **Throttle + lock.** No matter how many times you run `console` in a
  minute, Device42 is contacted at most once every 5 minutes. Concurrent
  invocations are serialized.
- **Never overwrite local edits.** The merge is strictly additive.
  Disagreements with Device42 are logged, never applied.
- **Backups on every merge.** First run of a new merge backs up
  `console_devices.csv` and `pdu_mapping.json` next to the originals
  (`*.bak.<TIMESTAMP>`). They're gitignored.

## License

MIT — see `LICENSE`.
