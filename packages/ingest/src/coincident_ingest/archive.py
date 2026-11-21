"""Raw archiver for Coincident.

Stores every fetched file losslessly (gzip of the original bytes; sha256 is of
the ORIGINAL bytes) and appends one JSON line per stored file to a manifest.
No parsing happens here. Stdlib only, Python >= 3.9, so it runs on a bare server.

Usage:
    python3 archive.py run      # one pass over all sources (what the timer calls)
    python3 archive.py status   # per-report file counts and last fetch time

Env:
    COINCIDENT_ARCHIVE   archive root (default /srv/coincident/raw)
    COINCIDENT_WORKERS   parallel downloads (default 4)
    COINCIDENT_REPORTS   optional comma-separated subset of IESO reports

IESO naming (verified 2026-11-16 against reports-public.ieso.ca/public/):
    PUB_<Report>_<date>_v<N>.<ext>   immutable versioned file  -> stored once
    PUB_<Report>_<date>.<ext>        alias of the latest version -> skipped
    PUB_<Report>.<ext>               alias of the latest file    -> skipped
    Unversioned files with no versioned sibling are stored as snapshots
    whenever their listing (mtime, size) changes.
"""

from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

USER_AGENT = "coincident-archiver/0.1"
IESO_BASE = "https://reports-public.ieso.ca/public/"

# Reports to mirror. Chosen for demand, forecasts, generation/fuel, ICI, and
# post-MRP (May 2025+) prices. Nodal LMP reports (~15 GB) are deliberately left
# out. Add names here; nothing else needs to change.
IESO_REPORTS = [
    # Demand (actual + forecast)
    "Demand", "DemandZonal", "RealtimeDemandZonal", "RealtimeTotals",
    "PredispTotals", "DATotals", "Adequacy3", "OntarioElectEnergy",
    # Generation / fuel mix / variable generation forecasts
    "GenOutputCapability", "GenOutputCapabilityMonth", "GenOutputbyFuelHourly",
    "GenOutputbyFuelMonthly", "VGForecastSummary", "IntertieScheduleFlow",
    # ICI / Global Adjustment
    "ICIPeakTracker", "ICIDemand", "GlobalAdjustment",
    "TTDC-PeakSystemDemand-Prelim", "TTDC-PeakSystemDemand-Final",
    # Prices (Ontario Zonal Price regime, plus legacy HOEP summaries)
    "DAHourlyOntarioZonalPrice", "PredispHourlyOntarioZonalPrice",
    "RealtimeOntarioZonalPrice", "RealtimeZonalEnergyPrices",
    "DAHourlyZonal", "PredispHourlyZonal", "PriceHOEPAverage",
]
if os.environ.get("COINCIDENT_REPORTS"):  # comma-separated override, for testing
    IESO_REPORTS = os.environ["COINCIDENT_REPORTS"].split(",")

# Single URLs that are overwritten in place: snapshot when content changes.
# min_interval_s throttles sources whose body changes on every request.
CITIES = {  # population-weighted starter set
    "toronto": (43.6532, -79.3832),
    "ottawa": (45.4215, -75.6972),
    "hamilton": (43.2557, -79.8711),
    "london": (42.9849, -81.2453),
    "kitchener": (43.4516, -80.4925),
}
_OM_VARS = ",".join([
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "apparent_temperature", "precipitation", "cloud_cover",
    "wind_speed_10m", "wind_direction_10m", "shortwave_radiation",
])
OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=" + ",".join(str(c[0]) for c in CITIES.values())
    + "&longitude=" + ",".join(str(c[1]) for c in CITIES.values())
    + "&hourly=" + _OM_VARS + "&forecast_days=16&timezone=GMT"
)
SNAPSHOT_SOURCES = [
    # (source, report, url, filename, min_interval_s)
    # The IESO multi-day demand forecast (Ontario-Demand-multiday.ashx) was
    # removed: frozen at CreatedAt 2025-04-30 since the market change. IESO's
    # demand forecast comes from the Adequacy3 report instead (see decisions.md).
    ("open_meteo", "forecast", OPEN_METEO_URL,
     "forecast_" + "_".join(CITIES) + ".json", 55 * 60),
]

VERSIONED = re.compile(r"_v\d+(\.\w+)$")
LISTING_ROW = re.compile(
    r'<a href="([^"?/][^"]*)">[^<]*</a>\s+(\d{2}-\w{3}-\d{4} \d{2}:\d{2})\s+(\S+)'
)
DATE_TOKEN = re.compile(r"_(\d{4})(\d{2})?\d*(?:_v\d+)?\.\w+$")

ROOT = Path(os.environ.get("COINCIDENT_ARCHIVE", "/srv/coincident/raw"))
MANIFEST = ROOT / "manifest.jsonl"
WORKERS = int(os.environ.get("COINCIDENT_WORKERS", "4"))

_manifest_lock = threading.Lock()


def log(msg: str) -> None:
    print(msg, flush=True)


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# ---------------------------------------------------------------- HTTP

def http_get(url: str, attempts: int = 4) -> Tuple[bytes, Dict[str, str]]:
    last: Optional[Exception] = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.read(), {k.lower(): v for k, v in r.headers.items()}
        except urllib.error.HTTPError as e:
            if e.code < 500 and e.code != 429:
                raise
            last = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
        time.sleep(2 ** i * 3)
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}: {last}")


# ---------------------------------------------------------------- storage

def write_blob(rel: Path, data: bytes) -> None:
    """Atomically write gzip(data) to ROOT/rel. Never overwrites."""
    dest = ROOT / rel
    if dest.exists():
        raise FileExistsError(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + f".tmp-{uuid.uuid4().hex[:8]}")
    with open(tmp, "wb") as f:
        # mtime=0 -> same input always gives the same .gz bytes
        with gzip.GzipFile(fileobj=f, mode="wb", mtime=0) as gz:
            gz.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, dest)


def append_manifest(rec: dict) -> None:
    line = json.dumps(rec, sort_keys=True) + "\n"
    with _manifest_lock:
        with open(MANIFEST, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())


def load_manifest() -> List[dict]:
    if not MANIFEST.exists():
        return []
    out = []
    with open(MANIFEST, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                log(f"WARN manifest line {n} is corrupt; ignoring")
    return out


def store(source: str, report: str, url: str, filename: str, rel: Path,
          data: bytes, headers: Dict[str, str], kind: str,
          listing_mtime: Optional[str] = None, fetched_at: Optional[dt.datetime] = None) -> dict:
    fetched_at = fetched_at or utcnow()
    write_blob(rel, data)
    rec = {
        "id": str(uuid.uuid4()),
        "source": source,
        "report": report,
        "url": url,
        "filename": filename,
        "kind": kind,  # "versioned" (immutable upstream) or "snapshot"
        "fetched_at": fetched_at.isoformat(),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
        "storage_path": str(rel),
        "compression": "gzip",
        "listing_mtime": listing_mtime,  # as shown by IESO; timezone unverified
        "http_last_modified": headers.get("last-modified"),
        "http_etag": headers.get("etag"),
        "content_type": headers.get("content-type"),
    }
    append_manifest(rec)
    return rec


def shard_for(filename: str) -> str:
    m = DATE_TOKEN.search(filename)
    if not m:
        return "undated"
    return m.group(1) + ("/" + m.group(2) if m.group(2) else "")


def snapshot_rel(source: str, report: str, filename: str, t: dt.datetime) -> Path:
    return Path(source, report, t.strftime("%Y/%m/%d"),
                t.strftime("%Y%m%dT%H%M%S%fZ") + "__" + filename + ".gz")


# ---------------------------------------------------------------- IESO

def list_dir(report: str) -> List[Tuple[str, str, str]]:
    body, _ = http_get(IESO_BASE + report + "/")
    return LISTING_ROW.findall(body.decode("utf-8", "replace"))


def plan_ieso(report: str, rows: List[Tuple[str, str, str]], known_urls: set,
              last_listing: Dict[str, Tuple[str, str]]) -> List[Tuple[str, str, str]]:
    """Return [(filename, kind, listing_mtime)] to fetch."""
    names = [r[0] for r in rows]
    versioned = [n for n in names if VERSIONED.search(n)]
    aliases = set()
    for v in versioned:
        base = VERSIONED.sub(r"\1", v)                    # PUB_X_2026111623.xml
        aliases.add(base)
        aliases.add(re.sub(r"_\d{4,10}(\.\w+)$", r"\1", base))  # PUB_X.xml
    todo = []
    for name, mtime, size in rows:
        url = IESO_BASE + report + "/" + name
        if VERSIONED.search(name):
            if url not in known_urls:
                todo.append((name, "versioned", mtime))
        elif name in aliases:
            continue
        elif last_listing.get(url) != (mtime, size):
            todo.append((name, "snapshot", mtime + "|" + size))
    return todo


def fetch_ieso_file(report: str, name: str, kind: str, listing: str,
                    last_sha: Dict[str, str]) -> Optional[dict]:
    url = IESO_BASE + report + "/" + name
    data, headers = http_get(url)
    if kind == "versioned":
        rel = Path("ieso", report, shard_for(name), name + ".gz")
        if (ROOT / rel).exists():  # crash between write and manifest append
            log(f"WARN {rel} on disk but not in manifest; re-recording")
            (ROOT / rel).rename((ROOT / rel).with_name(rel.name + f".orphan-{uuid.uuid4().hex[:8]}"))
        return store("ieso", report, url, name, rel, data, headers, kind, listing)
    if last_sha.get(url) == hashlib.sha256(data).hexdigest():
        # content identical; record the listing so we don't refetch it
        append_manifest({"kind": "listing_seen", "url": url, "listing_mtime": listing,
                         "fetched_at": utcnow().isoformat()})
        return None
    t = utcnow()
    return store("ieso", report, url, name, snapshot_rel("ieso", report, name, t),
                 data, headers, kind, listing, t)


# ---------------------------------------------------------------- run

def run() -> int:
    ROOT.mkdir(parents=True, exist_ok=True)
    started = utcnow()
    recs = load_manifest()
    known_urls = {r["url"] for r in recs if r.get("kind") == "versioned"}
    last_sha: Dict[str, str] = {}
    last_listing: Dict[str, Tuple[str, str]] = {}
    last_fetch: Dict[str, dt.datetime] = {}
    for r in recs:
        if r.get("kind") in ("snapshot", "listing_seen"):
            if r.get("sha256"):
                last_sha[r["url"]] = r["sha256"]
            lm = r.get("listing_mtime") or ""
            if "|" in lm:
                last_listing[r["url"]] = tuple(lm.split("|", 1))  # type: ignore[assignment]
            last_fetch[r["url"]] = dt.datetime.fromisoformat(r["fetched_at"])

    errors = 0
    stored = 0

    # Snapshot URLs first: they are the ones that can't be recovered later.
    for source, report, url, filename, min_interval in SNAPSHOT_SOURCES:
        prev = last_fetch.get(url)
        if prev and (started - prev).total_seconds() < min_interval:
            continue
        try:
            data, headers = http_get(url)
            if last_sha.get(url) == hashlib.sha256(data).hexdigest():
                continue
            t = utcnow()
            store(source, report, url, filename, snapshot_rel(source, report, filename, t),
                  data, headers, "snapshot", fetched_at=t)
            stored += 1
            log(f"stored {source}/{report} ({len(data)} B)")
        except Exception as e:  # noqa: BLE001 - keep going on any source failure
            errors += 1
            log(f"ERROR {source}/{report}: {e}")

    jobs = []
    for report in IESO_REPORTS:
        try:
            rows = list_dir(report)
        except Exception as e:  # noqa: BLE001
            errors += 1
            log(f"ERROR listing {report}: {e}")
            continue
        if not rows:
            log(f"WARN {report}: directory listing is empty")
        todo = plan_ieso(report, rows, known_urls, last_listing)
        if todo:
            log(f"{report}: {len(todo)} new of {len(rows)} listed")
        jobs.extend((report, *t) for t in todo)

    with cf.ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(fetch_ieso_file, rep, name, kind, lst, last_sha): (rep, name)
                for rep, name, kind, lst in jobs}
        for i, fut in enumerate(cf.as_completed(futs), 1):
            rep, name = futs[fut]
            try:
                if fut.result():
                    stored += 1
            except Exception as e:  # noqa: BLE001
                errors += 1
                log(f"ERROR {rep}/{name}: {e}")
            if i % 500 == 0:
                log(f"progress {i}/{len(jobs)}")

    secs = (utcnow() - started).total_seconds()
    log(f"done: stored={stored} errors={errors} queued={len(jobs)} in {secs:.0f}s")
    return 1 if errors else 0


def status() -> int:
    per: Dict[str, List] = {}
    for r in load_manifest():
        if r.get("kind") not in ("versioned", "snapshot"):
            continue
        key = f"{r['source']}/{r['report']}"
        p = per.setdefault(key, [0, 0, ""])
        p[0] += 1
        p[1] += r["size"]
        p[2] = max(p[2], r["fetched_at"])
    now = utcnow()
    print(f"{'report':45} {'files':>7} {'raw MB':>9}  last fetch (age)")
    for key in sorted(per):
        n, size, last = per[key]
        age = now - dt.datetime.fromisoformat(last)
        print(f"{key:45} {n:7} {size / 1e6:9.1f}  {last[:16]}Z ({age.total_seconds() / 3600:.1f}h)")
    return 0


def main(argv: Iterable[str]) -> int:
    args = list(argv)
    cmd = args[0] if args else "run"
    if cmd == "run":
        return run()
    if cmd == "status":
        return status()
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
