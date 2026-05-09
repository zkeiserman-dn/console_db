v2026.05.05.3  (2026-05-05)
v2026.05.09.1  (2026-05-09)
  * Resilience: Mac wrapper (console_expect) now auto-restores console.py
    from git on the dev VM before each run if the working tree file was 
    deleted (a recurring accidental rm leaves python3 with "No such file").
    Uses `git restore` / `git checkout HEAD --` inside ~/console_db.


  * Fix: console_expect now handles SSH key auth correctly. The previous
    version blindly did `expect "assword:"; send "$pw\r"`, which worked
    only when the dev VM still asked for a password. Once your key was
    pushed, key auth succeeded silently, the remote command ran and
    exited, ssh closed - and the trailing `send` then fired into a
    closed channel, surfacing the cryptic
        send: spawn id exp6 not open
    The new flow waits for either a password prompt OR any other output
    (= key auth already worked) and proceeds either way.
  * Robustness: console_expect also auto-loads DN_SERVER_HOST /
    DN_SERVER_PASSWORD / DN_SERVER_USER from ~/.zssh.conf when they
    aren't exported in the current shell. Stale terminals that haven't
    re-sourced ~/.zshrc since install will now Just Work, instead of
    failing with "DN_SERVER_HOST is not set".

v2026.05.05.2  (2026-05-05)
  * UX: when the requested port is held in EXCLUSIVE mode by another user,
    the wrapper used to leave you stranded inside the SN9116CO menu staring
    at "Exclusive mode and port busy! Press Enter to continue..." with no
    indication of who held the port or how to recover. connect() now
    detects the busy banner, walks the chassis admin menus to gather the
    serial-port table and the active session list, reverse-DNS resolves
    each session's source IP, filters out our own dev-VM hop, and prints a
    clean "PORT BUSY: <serial> (<CS> port N)" summary listing the
    candidate session owners (oldest session first), then exits 2 instead
    of dropping into the menu's Press-Enter loop.

v2026.05.05.1  (2026-05-05)
  * Fix: connect() no longer hardcodes "3" for the Port Access menu slot.
    On SN9116CO console servers (and any other model that uses a different
    layout), Port Access is at slot 4. Hardcoding 3 dropped you into Port
    Settings instead, then sent the device port number as a sub-menu key.
    The fix parses the main menu and picks the slot whose label is
    "Port Access" (case-insensitive). Falls back to "3" if not found, so
    older consoles keep working.

v2026.04.27.7  (2026-04-27)
  * Public repo is live at https://github.com/zkeiserman-dn/console_db.
  * Future updates: bump __version__ + prepend a CHANGELOG.md section, then
    `git commit && git push`. Every Mac running console will see the
    "What's new" banner the next time you run `console <SERIAL>`.

v2026.04.27.6  (2026-04-27)
  * Public release prep: all secrets moved to environment variables. Each
    script (console.py, d42_merge.py, dump_d42_consoles.py) auto-loads
    ~/.console_env on startup so non-interactive SSH sessions still see
    the credentials without needing to source it from .bashrc.
  * Repo published at https://github.com/zkeiserman-dn/console_db
  * console_devices.csv, pdu_mapping.json, d42_merge.log, .console_last_seen
    and .console_env are now gitignored. Live lab data stays out of the repo.

v2026.04.27.5  (2026-04-27)
  * Silent on no-op runs: the per-run `console vX.Y.Z` line is gone. The
    "What's new" banner still appears the first run after a __version__ bump
    (and on a brand-new account); after that, console prints nothing of its
    own at startup.

v2026.04.27.4  (2026-04-27)
  * Quieter background sync: removed the per-run "[d42] syncing console + PDU
    DB ..." status line. The merge still fires on every invocation; it just
    runs silently now. Failures (e.g. SSH cannot reach the dev VM) still
    surface as a single line so they don't go unnoticed.

v2026.04.27.3  (2026-04-27)
  * Replaced the SSH-fetch auto-updater with a "what's new" banner.
    Reason: this script always runs on the dev VM (via the Mac SSH wrapper),
    so users always have the latest -- there is nothing to download. Instead,
    every time __version__ changes the banner shows the new CHANGELOG entry
    and stores the new version in ~/.console_last_seen.
  * Silence the banner with: export CONSOLE_SKIP_UPDATE_CHECK=1

v2026.04.27.2  (2026-04-27)
  * (test) Fake bump to verify the auto-update prompt works end-to-end.
  * No functional change vs v2026.04.27.1.

v2026.04.27.1  (2026-04-27)
  * Initial versioned release of `console`.
  * Background Device42 sync: every `console` invocation triggers
    /home/dn/console_db/d42_merge.py on the dev VM. The merge refreshes the
    local SQLite cache (~/device42_cache.sqlite) and adds any NEW console + PDU
    rows to console_devices.csv / pdu_mapping.json. It NEVER overwrites
    existing entries -- mismatches are only logged, on the assumption that any
    local change is a deliberate manual fix.
  * Throttle: the merge runs at most once per 5 minutes; concurrent invocations
    are serialized via a file lock. See /home/dn/console_db/d42_merge.log.
