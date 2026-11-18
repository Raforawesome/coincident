# Scope and goals

What this project is trying to prove, what "done" means for each part of it,
and how to pick the next thing to work on. There are no timelines here on
purpose. Work is tracked in GitHub issues and lands through pull requests.

Related: [brief.md](brief.md) (original brief), [decisions.md](decisions.md)
(design log).

## The question

**Can public data predict Ontario's ICI coincident peak hours well enough to
be worth acting on, and how many alert days does it take?**

Everything else serves that question:
- The demand forecast is the foundation the peak model builds on.
- The data platform makes the forecasts trustworthy (as-of correct,
  reproducible).
- The dashboard makes the results inspectable.

## Outcomes

These are the four things the finished project must be able to show. Each
section has a checklist of what "done" means, and any unchecked item is a
candidate for the next issue.

### 1. A live system that runs itself

Ingestion, parsing, inference and alerts all run on schedules, unattended,
for months.

- [x] Raw archiver stores every relevant IESO file and forecast snapshot
- [ ] Off-server backup of the raw archive, with a tested restore
- [ ] An alert fires when any source stops producing new files
- [ ] Postgres + TimescaleDB under migrations; the schema is rebuildable from zero
- [ ] Every `core.*` table can be rebuilt from the raw archive alone, with the same result
- [ ] Scheduled parsing and inference (Dagster) with visible run history
- [ ] Dashboard shows data freshness per source
- [ ] Deployed behind TLS on a real domain, redeployable with one command

### 2. An honest demand forecast, compared against IESO

- [ ] IESO actual demand (2002 onward) and IESO's own forecasts (bitemporal) in `core.*`
- [ ] Weather forecasts *as issued*, with `issued_at`, for a population-weighted set of cities
- [ ] A backtest harness: rolling origin; a forecast is a pure function of `(as_of, data available at as_of)`
- [ ] Leakage tests that fail if any feature uses data from after `as_of`
- [ ] Baselines: seasonal naive, same-hour-last-week, IESO's published forecast
- [ ] A gradient-boosted quantile model (p10/p50/p90), tracked in MLflow
- [ ] Error broken down by horizon, season, hour of day and temperature regime
- [ ] Interval calibration checked (p10–p90 coverage close to 80%)
- [ ] A results page that says plainly where we beat IESO and where we don't

### 3. The peak result (the headline)

- [ ] Labels: top-5 hours per base period from `ICIDemand/`, with the adjusted vs real-time demand difference documented
- [ ] Peak risk per day/hour for the next few days (`ml.peak_risk`), built on the demand forecast distribution and the season-to-date maximum
- [ ] An alert policy: given a cost per alert day and a value per captured peak, choose when to alert
- [ ] Per-base-period curves of "top-5 hours captured vs alert days called", for our model, the IESO Peak Tracker and simple MW-threshold rules
- [ ] Uncertainty shown honestly (there are only about 25 positive hours in the testable history)
- [ ] Live alerts (Discord/email, with a dry-run mode) **running before May 1, 2027**, the start of the next base period
- [ ] Every live alert and outcome logged in `ml.alerts`, so the live season can be scored afterwards
- [ ] Write-up after the season: what the backtest predicted vs what happened

### 4. A codebase a newcomer can follow

- [ ] CI: ruff, pyright, pytest, import-linter, and a check that the generated TS client is up to date
- [ ] Package boundaries enforced (see below)
- [ ] Parser tests built on real saved files, including DST-boundary days
- [ ] The README explains how to run every part locally
- [ ] Every contract-changing decision has a line in `decisions.md`

### Also in scope, once the above is moving

- **Carbon intensity:** gCO₂/kWh from generation by fuel × documented
  emission factors, plus "cleanest hours in the next 24h". It needs little
  modelling, so it's a good pick when other work is blocked.
- **Prices (stretch):** day-ahead vs real-time Ontario Zonal Price spread,
  using post–May 2025 data only (after the Market Renewal Program).

## Not now, and what would change that

These are scope choices for the current design, not permanent rules.

| Not now | Why | Revisit if |
|---|---|---|
| Languages beyond Python + TypeScript | One toolchain; everyone can read everything | A component has a measured performance problem Python can't fix |
| Message bus (Kafka etc.) | Data arrives every 5 min to daily; Postgres is enough as the integration point | We add sub-minute inference or several independent consumers of the same events |
| LLM-based "models" | Forecasts must come from models we can backtest and explain | Never for the forecasts themselves |
| Real-time online inference | Forecasts are precomputed; the API is read-only | A feature genuinely needs per-request computation |

## Architecture and boundaries

**Postgres tables are the interface between packages.** Packages agree on a
table's shape and don't import each other's internals.

```
ingest ──writes──> raw.*, core.*
features ─reads core.*──> features.* (or in-memory frames)
ml ──reads features.*──> ml.forecasts, ml.peak_risk, ml.backtest_*
api ──reads core.*, ml.* (read-only DB role)──> JSON
web ──reads the API only (generated TS client)
pipelines ──calls the packages' public functions; owns scheduling only
```

| Package | Owns | Public surface | Must not |
|---|---|---|---|
| `ingest` | Fetchers, raw archive, parsers | `archive.run()`; one `parse_<report>(bytes) -> rows` per report | Import `features`/`ml`; do network I/O inside parsers |
| `features` | As-of feature construction | `build(as_of, targets) -> DataFrame` | Read any row with an `available_at` later than `as_of` |
| `ml` | Models, backtests, MLflow | A small `Forecaster` protocol (`fit`, `predict(as_of)`), a backtest runner | Query `raw.*`; know about HTTP |
| `api` | Read-only HTTP | FastAPI routes, Pydantic models (exported to OpenAPI) | Write to the DB; do heavy computation |
| `pipelines` | Dagster assets, schedules, sensors | Asset definitions | Contain business logic |
| `web` | Dashboard | Pages, charts | Talk to Postgres; hand-write API types |

Principles:

- **Pure core, thin I/O shell.** Parsers take bytes and return rows. Feature
  builders take frames and an `as_of`. Models take arrays. Database and
  network code sits at the edges, so the core is testable without a
  database.
- **Every `core.*` row records** where it came from (`source_file_id`) and
  when it became known (`available_at`).
- **Boring over clever.** Use plain SQL via psycopg and polars frames, in
  small modules. Add an abstraction when a second caller needs it.
- **The raw archive is append-only and sacred.** No code path deletes or
  rewrites it.

## Measured constraints

Measured on 2026-11-16/17; re-check them before relying on them.

- **Weather forecast history:** Open-Meteo's historical-forecast API has
  values from about early 2021, and earlier dates return nulls. That gives
  about 5 complete base periods (2021–22 to 2025–26) for weather-driven
  backtests.
- **Weather lead time:** that API appears to stitch together the first hours
  of successive model runs. If so, it is optimistic as a *day-ahead* input.
  This must be confirmed or ruled out before any model uses it.
- **IESO history:** `ICIPeakTracker/` goes back to 2022, `ICIDemand/` to 2021,
  and `Demand/` (hourly actuals) to 2002.
- **Discontinued report:** `RealtimeConstTotals/` is empty; `RealtimeTotals/`
  replaces it.

## Open questions

Each of these should become an issue when someone picks it up.

- IESO time convention: hour-ending, EST vs EDT, and how DST days look in
  each report.
- A lead-time-correct source for historical weather forecasts.
- Exactly which demand series ICI uses for peak determination, and how far
  adjusted values move after the fact.
- The current ICI program rules and how they changed over the testable
  period.
- Peak-label stability: how often the top-5 changes late in a base period.

## When you don't know what to do next

Go down this list and take the first one that applies:

1. **Is any data at risk?** That means backup, monitoring, or a source the
   archiver misses. Fix it first, because lost data can't be recovered.
2. **Is someone blocked on you?** Usually the blocker is a `core.*` table or
   an interface another workstream needs. Unblock them.
3. **What's the next unchecked box in Outcome 3?** The peak result is the
   point of the project. Work backwards from what it needs.
4. **What's the biggest error source** in the latest backtest? Error analysis
   beats adding features blindly.
5. **Can a newcomer follow it?** Missing tests, a confusing module, a
   decision nobody wrote down.
6. **Is something else bothering you?** Carbon intensity, dashboard polish,
   prices.

## Working agreement

- **One issue per unit of work**, with a "done when" line. Open questions
  get issues too.
- **Small PRs**, linked to their issue and reviewed by someone who didn't
  write them, with CI green before merge.
- **No parser without a real-file fixture. No feature without a leakage test.**
- If a change alters a contract (a table shape, a public function, an API
  route), it also updates `decisions.md`.
