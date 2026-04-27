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
