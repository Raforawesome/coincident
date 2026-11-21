# Decisions log

Newest first. Each entry: what we decided, why, and what would change it.

## 2026-11-17 — IESO time convention verified

**Finding.** IESO uses **hour-ending (HE 1–24) in fixed Eastern Standard
Time (UTC−5) all year**. It does not switch to daylight time.

**Evidence** (checked 2026-11-18 03:30 UTC):
- `PUB_Demand_2025.csv` and `PUB_Demand_2026.csv` have exactly 24 rows on
  every day, including the DST days (2026-03-08, 2026-11-01). Local
  wall-clock time would give 23 and 25 rows.
- `PUB_RealtimeTotals_2026111723_v6.xml` (`DeliveryHour` 23) has
  `CreatedAt` 22:23:16 and was listed at 22:26. So hour 23 is the hour
  ending 23:00 (22:00–23:00 EST).
- Directory listing times and in-file "Created at" times are also EST, even in
  summer:
  - `GenOutputCapabilityMonth_202606_v30.csv`: created 06:00:08, listed 06:01,
    HTTP `Last-Modified` 11:01:16 GMT on 2026-07-01 (EDT would have been 10:01).
  - `GenOutputbyFuelMonthly_2026_v10.xml`: listed 06:33, `Last-Modified`
    11:33:19 GMT on 2026-10-29 (still EDT that day).

**Conversion rule for parsers.**
`hour_start_utc = (DeliveryDate 00:00) + (HE − 1) h + 5 h`, and
`hour_end_utc = hour_start_utc + 1 h`. No timezone library or DST logic is
needed. Do **not** use `America/Toronto`. A 5-minute interval `i` (1–12)
within HE `h` starts at `hour_start_utc + 5·(i − 1)` minutes.

**Still to decide.** Whether `core.*` `ts` columns hold interval start or
interval end. Pick one and write it in the schema comments. Interval start is
the more common choice (gridstatus uses it).

**Other findings**
- **The multi-day forecast snapshot source is dead.** `Ontario-Demand-multiday.ashx`
  still returns a file with `CreatedAt 2025-04-30T20:12:16`. It stopped
  updating at the market change. **Removed from `SNAPSHOT_SOURCES`.** Any copies
  already archived on the server are left alone, since the archive is
  append-only.
- **Use `Adequacy3` instead for IESO's demand forecast.** It's already in the
  archive list. Each delivery date has many versions (72 listed for
  2026-11-18), each with a `CreatedAt` (that's `issued_at`) and
  `ForecastOntDemand` by `DeliveryHour`. That gives us exactly the
  `(issued_at, target_time)` history the brief wants, from a source that keeps
  every version.
- **Known gap:** `PUB_Demand_2025.csv` is missing 2025-05-01 HE1, the first hour
  of the new market. The parser should allow it, and a data-quality check
  should report it rather than fail.

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
