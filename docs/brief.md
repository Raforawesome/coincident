# Project Brief: Ontario Grid Peak Forecaster (working name: "Coincident")

> Name is not final; `coincident` is the package/repo slug for now.

## What this is

A forecasting system for Ontario's electricity grid, built from public data. It ingests public IESO grid data and weather data, stores it in Postgres, trains ML models, and serves forecasts through a Next.js dashboard.

**Headline feature:** predicting Ontario's **ICI coincident peak hours**, the 5 highest-demand hours in each May 1 – April 30 base period. Large "Class A" electricity customers pay Global Adjustment based on their share of demand during those 5 hours, so predicting them (and deciding when to issue curtailment alerts) has real economic value. Peaks are only known after the fact, so this is a rare-event classification and decision problem.

**Supporting features:**
1. Day-ahead hourly Ontario demand forecast (the foundation; peak prediction builds on it).
2. Grid carbon intensity (gCO₂/kWh from generation by fuel × emission factors), with "cleanest hours in the next 24h".
3. Stretch: day-ahead vs real-time price spread / price spikes.

**Baselines to compare against (not beat at all costs):** IESO publishes its own demand forecast and an ICI "Peak Tracker". The project's value is a rigorous, honest comparison and the decision layer: *given curtailment costs, how many alert days should be called?* Error analysis may later reveal a distinctive angle. Don't chase novelty up front.

## Goals and non-goals

**Goals**
- Ship end to end and deploy early; iterate in vertical slices.
- Rigor: honest backtests, no leakage, documented model decisions.

**Non-goals**
- No Rust. Python plus TypeScript only.
- No message bus or event-driven microservices (Kafka, NATS, etc.). This is a time-triggered batch system; Postgres is the integration point.
- No LLM-API "ML". Models are classical/tabular (baselines, gradient boosting, quantile regression).
- No real-time online inference. Forecasts are precomputed on a schedule and served read-only.

## Critical design constraints

1. **Archive raw data immediately.** Several IESO detailed reports only keep a rolling ~30-day window (e.g., hourly generator output/capability, real-time totals). The fetcher must store every raw file byte-for-byte (immutable), with parsing as a separate, re-runnable step. A parser bug must never lose data. **This is the first thing to build.**
2. **Bitemporal forecasts.** Every forecast table (weather forecasts, IESO forecasts, our model outputs) is keyed by `(issued_at, target_time)`. Features must be built "as of" `issued_at` so training can never see information from after the simulated decision time.
3. **No weather leakage.** Train on archived weather *forecasts* (as they were issued), not on actual observed weather. Store actual weather separately for evaluation only.
4. **Market regime change.** IESO's Market Renewal Program went live **May 1, 2025**: HOEP was replaced by the Ontario Zonal Price, and a Day-Ahead Market was introduced. Price data before and after that date are different regimes. Demand data is unaffected. Handle this explicitly in any price work.
5. **Time handling.** Store everything as UTC `timestamptz`. IESO reports use an hour-ending convention (hours 1–24) in a fixed local standard time; **verify the exact convention before writing parsers** and cover it with tests (DST boundaries especially).

## Data sources

- **IESO public reports:** `https://reports-public.ieso.ca/public/` (about 139 report directories; mostly XML, some CSV). Relevant:
  - `RealtimeConstTotals/`: 5-min/hourly Ontario and market demand (XML has Ontario demand; CSV may not)
  - `GenOutputCapability/`: hourly output per generator (≥20 MW), with forecasts for wind/solar
  - `GenOutputbyFuelHourly/`: historical output by fuel
  - `OntarioZonalDemand/`: zonal demand forecast up to 34 days out
  - ~~Multi-day Ontario demand forecast (`Ontario-Demand-multiday.ashx`)~~: frozen since 2025-04-30. Use `Adequacy3/` for IESO's demand forecast (see decisions.md).
  - Long historical demand CSVs (back to 2002) via the IESO Data Directory page
  - ICI Peak Tracker: `https://www.ieso.ca/peaktracker`
- **`gridstatus` Python library:** already wraps many IESO endpoints. Use it as a reference or for quick starts, but our fetchers must still archive raw files ourselves.
- **Weather:** Open-Meteo (forecast, historical, and possibly historical-forecast archive). **Verify how far back archived forecasts go.** Environment Canada is the fallback.

## Stack

| Layer | Choice |
|---|---|
| Python tooling | `uv` workspace, ruff, pyright, pytest |
| Orchestration | Dagster (date-partitioned assets for backfills; sensors for "new file arrived") |
| Raw archive | Filesystem (or MinIO) + manifest table in Postgres |
| Database | Postgres + TimescaleDB; schemas `raw`, `core`, `features`, `ml` |
| Migrations | Alembic (or dbmate) |
| Data access | psycopg3 + polars; no ORM |
| ML | scikit-learn baselines, LightGBM, quantile regression; MLflow (self-hosted) for tracking and model registry |
| API | FastAPI + Pydantic, read-only |
| API types | `openapi-typescript` generates the TS client from FastAPI's OpenAPI schema |
| Frontend | Next.js (App Router), TypeScript, Tailwind, uPlot or ECharts; pnpm |
| Alerts | Discord webhook / email when peak risk crosses a threshold |
| Infra | Docker Compose on a self-managed Fedora server; Caddy for TLS; GitHub Actions CI; nightly `pg_dump` backups off-box |

## Architecture / data flow

```
IESO reports ─┐
Open-Meteo ───┼─> fetchers ─> raw archive ─> parsers ─> core.* (clean time series)
                                                          │
                                                          v
                                             features.* (as-of-correct)
                                                          │
                                   train (weekly) <───────┤
                                        │                 v
                                     MLflow ──> infer (on each forecast update)
                                                          │
                                                          v
                                              ml.forecasts, ml.peak_risk
                                                          │
                                       FastAPI ──> Next.js dashboard
                                                └──> alerts
```

## Suggested repo layout

```
coincident/
  pyproject.toml            # uv workspace root
  packages/
    ingest/                 # fetchers, raw archive, parsers
    features/               # as-of feature builders
    ml/                     # models, backtest harness, MLflow integration
    api/                    # FastAPI app
    pipelines/              # Dagster definitions (assets, schedules, sensors)
  db/migrations/
  web/                      # Next.js app (pnpm)
  infra/                    # docker-compose, Caddyfile, backup scripts
  docs/                     # decisions log, model reports
```

## Initial schema sketch (adjust as needed)

- `raw.fetch_manifest(id, source, report, url, fetched_at, sha256, storage_path, parse_status, parse_error)`
- `core.demand_actual(ts, ontario_demand_mw, market_demand_mw, source_file_id)`: hypertable
- `core.ieso_demand_forecast(issued_at, target_time, demand_mw, source_file_id)`
- `core.gen_by_fuel(ts, fuel, output_mw, source_file_id)`
- `core.weather_forecast(issued_at, target_time, location, temp_c, humidity, dewpoint_c, wind, cloud_cover, ...)`
- `core.weather_actual(ts, location, ...)`: evaluation only
- `ml.forecasts(model_version, issued_at, target_time, p10, p50, p90)`
- `ml.peak_risk(model_version, issued_at, target_date, target_hour, probability)`
- `ml.alerts(issued_at, target_date, threshold, sent_at, channel)`

Weather locations: start with a small set of population-weighted Ontario cities (e.g., Toronto, Ottawa, Hamilton, London, Kitchener-Waterloo).

## Build order (vertical slices; each one deployable)

1. **Fetch and archive** IESO demand + IESO demand forecast → parse into `core` → one FastAPI endpoint → one Next.js chart (actual vs IESO forecast). Deploy. Schedule the fetcher so archiving starts ASAP.
2. **Weather ingestion** with `issued_at`; historical backfill via Dagster partitions.
3. **Backtest harness:** rolling-origin evaluation; naive baselines (same hour last week, etc.); comparison vs IESO's forecast. Metrics: MAE/MAPE for demand; for peaks, recall of top-5 hours vs number of alert days.
4. **LightGBM demand model** with quantile outputs, tracked in MLflow.
5. **Peak classifier + alert policy:** probability per day/hour of being a top-5 hour; cost-aware threshold selection; compare vs IESO Peak Tracker; Discord/email alerts.
6. **Fuel mix + carbon intensity** views; "cleanest hours next 24h" widget.
7. Stretch: DA/RT price spread (post–May 2025 data only, or explicit regime handling).

Dashboard views: forecast vs actual with uncertainty bands, fuel mix stacked area, cleanest hours, peak-risk indicator (seasonal), backtest explorer (pick date range and model).

## Working conventions

- Work in small, scoped vertical slices; propose a plan for each slice before implementing.
- Modular boundaries between packages; no cross-package imports except through defined interfaces.
- Tests required for parsers (use saved real sample files as fixtures) and for the backtest harness (leakage tests: assert features never use data after `issued_at`).
- Keep a `docs/decisions.md` log of design choices and model results (this feeds the eventual writeup).

## Open items to verify before relying on them

- IESO report time convention (hour-ending, fixed standard time vs DST) and exact retention window per report.
- Open-Meteo historical-forecast archive coverage and terms of use.
- Current ICI rules (Ontario has adjusted the program over time); IESO Peak Tracker shows top-10 candidates; the payment basis is top-5.
- Adjusted vs real-time demand values used for ICI peak determination (adjusted values can shift by hundreds of MW, so they're a label-noise source).
