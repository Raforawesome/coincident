# Decisions log

Newest first. Each entry: what we decided, why, and what would change it.

## 2026-11-17 — Raw archiver before anything else

**Decision.** Ship a stdlib-only archiver running on a systemd timer (every
15 minutes) before building the schema, Dagster, or parsers.

**Why.** Several IESO reports keep only about 30 days of files, and the IESO
multi-day demand forecast and weather forecasts are overwritten in place. Any
day without archiving loses data permanently. Parsing can always be re-run
later.

**Details.**
- *Storage:* raw bytes are stored gzip-compressed (lossless). `sha256` is of
  the original bytes. Files are never overwritten.
- *Manifest:* starts as `manifest.jsonl` next to the archive and will be
  loaded into `raw.fetch_manifest` once Postgres exists. A line is appended
  only after its file has been fsynced and renamed into place.
- *IESO versioning:* verified 2026-11-16 against the directory listings.
  - `PUB_<Report>_<date>_v<N>.<ext>` files are immutable versions, stored once.
  - `PUB_<Report>_<date>.<ext>` and `PUB_<Report>.<ext>` are aliases of the
    latest version and are skipped.
  - Unversioned files with no versioned sibling are snapshotted whenever their
    listed mtime or size changes.
- *Server Python:* runs on the stock Python 3.9 on Oracle Linux 9 so it has no
  dependencies to install.

**Report selection.** 27 reports covering demand, Adequacy3 forecasts,
generation and fuel mix, the variable-generation forecast, ICI, and
zonal/HOEP prices. Nodal LMP reports (~15 GB currently listed) are excluded.
The selected reports total roughly 3 GB uncompressed at current listing size.

**Findings vs the brief.**
- `RealtimeConstTotals/` is empty, apparently since the May 2025 market
  change. `RealtimeTotals/` (5-minute, XML and CSV) appears to replace it.
- There is no `OntarioZonalDemand/` directory. The closest are `DemandZonal/`
  and `RealtimeDemandZonal/`.
- The ICI Peak Tracker is published as a report (`ICIPeakTracker/`, hourly
  versions), alongside `ICIDemand/`. No web scraping is needed.

**Open questions.**
- *Time convention:* the timezone of the listing mtimes and of report hour
  conventions is still unverified (needed before writing parsers).
- *Open-Meteo `issued_at`:* fetch time is used as the issue-time proxy. The
  model run time isn't in the response.
- *Backup:* there is no off-server backup of the archive yet.
