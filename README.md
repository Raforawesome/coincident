# Coincident

Forecasting Ontario's electricity grid from public IESO and weather data, with
one headline question: **which hours will be Ontario's ICI coincident peaks?**

Large "Class A" customers pay Global Adjustment in proportion to their demand
during the 5 highest-demand hours of each May–April base period. Those hours
are only known after the fact, so predicting them and deciding when to call
curtailment alerts is a rare-event forecasting and decision problem.

Planned pieces:

- Day-ahead hourly Ontario demand forecast, compared against IESO's own
- ICI peak-hour probabilities plus a cost-aware alert policy, compared
  against the IESO Peak Tracker
- Grid carbon intensity and "cleanest hours in the next 24h"

See [docs/scope.md](docs/scope.md) for scope and goals,
[docs/brief.md](docs/brief.md) for the full brief, and
[docs/decisions.md](docs/decisions.md) for the design log.

## Status

- [x] **Raw archiver** running on the server every 15 minutes
- [ ] Parsers into Postgres (`core.*`)
- [ ] API and first dashboard chart (actual vs IESO forecast)
- [ ] Weather backfill, backtest harness, models

## Raw archiver

[`packages/ingest/src/coincident_ingest/archive.py`](packages/ingest/src/coincident_ingest/archive.py)
mirrors selected IESO public report directories plus overwrite-in-place
sources (currently Open-Meteo forecasts). Several IESO
reports keep only about 30 days of history, so this runs before anything else
exists. It is standard-library-only Python (3.9+), with no parsing.

- Each file is stored gzip-compressed under `ieso/<Report>/<YYYY>/<MM>/`.
  The recorded `sha256` is of the original, uncompressed bytes.
- Every stored file gets one line in `manifest.jsonl`: url, fetched_at (UTC),
  sha256, size, storage path, HTTP headers.
- Upstream versioned files (`_vN`) are immutable and stored once. Unversioned
  "latest" aliases are skipped. Sources that change in place are snapshotted
  whenever their content changes.

```bash
# one pass (what the timer runs)
COINCIDENT_ARCHIVE=./raw python3 packages/ingest/src/coincident_ingest/archive.py run

# per-report file counts and last fetch time
COINCIDENT_ARCHIVE=./raw python3 packages/ingest/src/coincident_ingest/archive.py status

# try a small subset locally
COINCIDENT_ARCHIVE=./raw COINCIDENT_REPORTS=ICIPeakTracker,Demand \
  python3 packages/ingest/src/coincident_ingest/archive.py run
```

Deploying to the server (systemd timer, dedicated user, SELinux-friendly paths)
is covered in [`infra/install-archiver.sh`](infra/install-archiver.sh).

## Layout

```
packages/ingest/   fetchers, raw archive, (soon) parsers
infra/             systemd units, install scripts, (soon) docker-compose
docs/              brief, decisions log
```

## Data sources

- IESO public reports, <https://reports-public.ieso.ca/public/>
- Open-Meteo forecast API, <https://open-meteo.com/> (non-commercial use)
