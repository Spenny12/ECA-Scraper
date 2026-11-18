"""Runs the enrichment pipeline on your own PC, in chunks, with health checks.

Does the same lookups as the website (Companies House, Serper, Hunter,
FullEnrich) but never pushes to HubSpot. Keys come from the .env file.

    python scripts/run_pipeline_local.py "upload.csv" --out-dir "runs/my-run"

Each chunk of companies is saved as chunk_NNNN.csv in the output folder as
soon as it finishes, so re-running the same command after a crash or a stop
carries on from the next chunk. results.csv is rebuilt from all chunks at the
end (and whenever you pass --combine-only).

The run stops itself, with the reason in status.json and progress.log, when:
- an API rejects the key or reports no credits (HTTP 401, 402 or 403),
- credits fall below the --min-* floors before a chunk starts,
- a chunk's results look broken (e.g. Companies House finds almost nothing,
  Serper finds no domains, or most calls to a service fail).
"""
import argparse
import asyncio
import json
import logging
import re
import sys
import time
from collections import Counter
from pathlib import Path

import httpx
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import companies_house  # noqa: E402
import fullenrich  # noqa: E402
import hunter  # noqa: E402
import pipeline  # noqa: E402
import serper  # noqa: E402
from config import CH_API_KEY, FULLENRICH_API_KEY, HUNTER_API_KEY, SERPER_API_KEY, logger  # noqa: E402
from models import PipelineJob  # noqa: E402

COLUMNS = pipeline.CSV_COLUMNS + ["Title Number"]
FATAL_STATUSES = {401, 402, 403}
SERVICE_RE = {
    "Companies House": re.compile(r"^(CH |Companies House)"),
    "Serper": re.compile(r"^Serper"),
    "Hunter": re.compile(r"^Hunter"),
    "FullEnrich": re.compile(r"^FullEnrich"),
}


class StopRun(Exception):
    """The run must stop - the message says why."""


class ErrorCounter(logging.Handler):
    """Counts the pipeline's warnings per service and HTTP status, so a
    failing API shows up as numbers instead of rows of "not found"."""

    def __init__(self):
        super().__init__(logging.WARNING)
        self.counts = Counter()

    def emit(self, record):
        msg = record.getMessage()
        service = next((s for s, rx in SERVICE_RE.items() if rx.search(msg)), "other")
        m = re.search(r"HTTP (\d{3})", msg)
        status = int(m.group(1)) if m else None
        if "CREDITS_INSUFFICIENT" in msg:
            status = 402
        if status is None:
            if not re.search(r"failed|rate limit|didn't finish", msg):
                return  # e.g. "name too different" - a normal no-match, not an error
            status = 0  # a network error or timeout
        # A direct Companies House lookup by number returning 404 just means
        # the number is wrong or old; the name search runs next. Not an error.
        if service == "Companies House" and status == 404:
            return
        self.counts[(service, status)] += 1

    def take(self):
        counts, self.counts = self.counts, Counter()
        return counts


def balances(client: httpx.Client) -> dict:
    """Credits left on each paid service (None if the check itself failed)."""
    out = {}
    try:
        r = client.get("https://api.hunter.io/v2/account", params={"api_key": HUNTER_API_KEY})
        out["Hunter"] = r.json()["data"]["requests"]["credits"]["remaining"] if r.status_code == 200 else None
    except Exception:
        out["Hunter"] = None
    try:
        r = client.get("https://google.serper.dev/account", headers={"X-API-KEY": SERPER_API_KEY})
        out["Serper"] = r.json().get("balance") if r.status_code == 200 else None
    except Exception:
        out["Serper"] = None
    if FULLENRICH_API_KEY:
        try:
            r = client.get("https://app.fullenrich.com/api/v2/account/credits", headers={"Authorization": f"Bearer {FULLENRICH_API_KEY}"})
            out["FullEnrich"] = r.json().get("balance") if r.status_code == 200 else None
        except Exception:
            out["FullEnrich"] = None
    return out


def title_lookup(csv_path: Path) -> dict:
    """{(company, address, purchase date): Title Number} from the input file,
    so each output row can carry the Land Registry title it came from."""
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    if "Title Number" not in df.columns:
        return {}
    df["Purchase Date"] = pipeline._derive_purchase_date(df)
    out = {}
    for _, r in df.iterrows():
        for n in range(1, 5):
            name = r.get(f"Proprietor Name ({n})", "").strip()
            if name:
                out[(name.upper(), r["Property Address"].strip().upper(), r["Purchase Date"])] = r["Title Number"].strip()
    return out


def check_stage(stage: str, counts: Counter, calls: int):
    """Stops the run if a service rejected the key or most calls failed."""
    for (service, status), n in counts.items():
        if status in FATAL_STATUSES:
            raise StopRun(f"{service} returned HTTP {status} {n} time(s) during {stage} - check the key and credits.")
    # Companies House and FullEnrich wait and retry after a 429, so those aren't lost lookups.
    retried = stage in ("Companies House", "FullEnrich")
    failed = sum(n for (service, status), n in counts.items() if service == stage and not (retried and status == 429))
    if calls >= 20 and failed / calls > 0.2:
        raise StopRun(f"{failed} of {calls} {stage} calls failed in this chunk ({dict(counts)}).")


def check_results(stats: dict):
    """Stops the run if the chunk's output looks like a silent failure."""
    n = stats["companies"]
    if n < 40:
        return  # too few to judge (e.g. the last chunk)
    if stats["ch_found"] / n < 0.5:
        raise StopRun(f"Companies House found only {stats['ch_found']} of {n} companies.")
    if stats["domains"] == 0:
        raise StopRun(f"Serper found no domains for {n} companies.")
    if stats["hunter_lookups"] >= 30 and stats["emails"] == 0:
        raise StopRun(f"Hunter found no emails in {stats['hunter_lookups']} lookups.")


async def run_chunk(names, metadata, max_directors, label, counter) -> tuple:
    job = PipelineJob(id=label, status="Running", created_at=time.time(), updated_at=time.time())
    async with httpx.AsyncClient(timeout=30.0) as client:
        ch_data = await companies_house.enrich_companies(job, client, names, metadata, CH_API_KEY)
        check_stage("Companies House", counter.counts, len(names))
        ch_map = {c.company_name: c for c in ch_data}

        locality = {n: c.registered_locality for n, c in ch_map.items() if c.registered_locality}
        domains = await serper.discover_domains(job, client, names, SERPER_API_KEY, locality)
        check_stage("Serper", counter.counts, len(names))
        dom_map = {d.provided_name: d.discovered_domain for d in domains}

        lookups = sum(min(len(ch_map[n].directors), max_directors) for n in names
                      if dom_map.get(n) and n in ch_map and ch_map[n].directors)
        contacts = await hunter.discover_director_emails(job, client, names, dom_map, ch_map, HUNTER_API_KEY, max_directors) if HUNTER_API_KEY else {}
        check_stage("Hunter", counter.counts, lookups)

        results = pipeline.combine_results(names, domains, contacts, ch_data, metadata)
        if FULLENRICH_API_KEY:
            await fullenrich.enrich_rows(job, client, results, FULLENRICH_API_KEY)
            check_stage("FullEnrich", counter.counts, 0)

    stats = {
        "companies": len(names),
        "ch_found": sum(1 for c in ch_data if c.company_number),
        "domains": sum(1 for d in domains if d.discovered_domain),
        "hunter_lookups": lookups,
        "emails": sum(len(c.emails_found) for c in contacts.values()),
        "phones": len({(r["Employee Name"], r["Target Company Name"]) for r in results if str(r.get("Phone Number") or "").strip()}),
        "rows": len(results),
        "errors": {f"{s} {st or ''}".strip(): n for (s, st), n in counter.take().items()},
    }
    return results, stats


def combine(out_dir: Path) -> Path:
    files = sorted(out_dir.glob("chunk_*.csv"))
    frames = [pd.read_csv(f, dtype=str, keep_default_na=False) for f in files]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)
    path = out_dir / "results.csv"
    df.reindex(columns=COLUMNS).to_csv(path, index=False, encoding="utf-8")
    return path


def main():
    ap = argparse.ArgumentParser(description="Run the enrichment pipeline locally, in resumable chunks.")
    ap.add_argument("input", help="CSV in the Land Registry (CCOD) layout")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--chunk-size", type=int, default=250, help="companies per chunk (default 250)")
    ap.add_argument("--max-directors", type=int, default=3, help="Hunter lookups per company (default 3)")
    ap.add_argument("--min-hunter-credits", type=int, default=300)
    ap.add_argument("--min-serper-credits", type=int, default=300)
    ap.add_argument("--min-fullenrich-credits", type=int, default=300)
    ap.add_argument("--dry-run", action="store_true", help="count companies and chunks, check credits, then stop")
    ap.add_argument("--combine-only", action="store_true", help="just rebuild results.csv from the finished chunks")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.combine_only:
        print(f"Combined into {combine(out_dir)}")
        return

    file_handler = logging.FileHandler(out_dir / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    counter = ErrorCounter()
    logger.addHandler(counter)
    progress = open(out_dir / "progress.log", "a", encoding="utf-8")

    def say(msg):
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
        print(line, flush=True)
        progress.write(line + "\n")
        progress.flush()

    def write_status(**kw):
        (out_dir / "status.json").write_text(json.dumps({"updated": time.strftime("%Y-%m-%d %H:%M:%S"), **kw}, indent=2), encoding="utf-8")

    missing = [k for k, v in {"CH_API_KEY": CH_API_KEY, "SERPER_API_KEY": SERPER_API_KEY, "HUNTER_API_KEY": HUNTER_API_KEY}.items() if not v]
    if missing:
        raise SystemExit(f"Missing in .env: {', '.join(missing)}")

    input_path = Path(args.input)
    job = PipelineJob(id="plan", status="x", created_at=0, updated_at=0)
    names, metadata = pipeline.preprocess_uploaded_file(job, input_path.read_bytes(), input_path.name)
    titles = title_lookup(input_path)
    chunks = [names[i:i + args.chunk_size] for i in range(0, len(names), args.chunk_size)]
    done = {int(p.stem.split("_")[1]) for p in out_dir.glob("chunk_*.csv")}
    props = sum(len(m["properties"]) for m in metadata.values())

    with httpx.Client(timeout=30) as http:
        bal = balances(http)
    say(f"START {input_path.name}: {len(names)} companies, {props} properties, {len(chunks)} chunks of {args.chunk_size}"
        f" ({len(done)} already done) | credits {bal}")
    if args.dry_run:
        need = sum(len(c) for i, c in enumerate(chunks, 1) if i not in done)
        say(f"DRY RUN: {need} companies to process. Worst case Hunter lookups: {need * args.max_directors}; Serper searches: about {need}.")
        return

    totals = Counter()
    started = time.time()
    try:
        for i, chunk in enumerate(chunks, 1):
            if i in done:
                continue
            with httpx.Client(timeout=30) as http:
                bal = balances(http)
            for service, floor in (("Hunter", args.min_hunter_credits), ("Serper", args.min_serper_credits),
                                   ("FullEnrich", args.min_fullenrich_credits)):
                if service in bal and bal[service] is not None and bal[service] < floor:
                    raise StopRun(f"{service} has {bal[service]} credits left, below the floor of {floor}.")
            t0 = time.time()
            write_status(state="running", chunk=i, chunks=len(chunks), credits=bal, totals=dict(totals))
            results, stats = asyncio.run(run_chunk(chunk, metadata, args.max_directors, f"{out_dir.name}-{i:04d}", counter))
            for r in results:
                key = (r["Target Company Name"].upper(), str(r["Property Address"]).strip().upper(), r["Purchase Date"])
                r["Title Number"] = titles.get(key, "")
            frame = pd.DataFrame(results).reindex(columns=COLUMNS)
            try:
                check_results(stats)
            except StopRun:
                # Kept for a look, but not counted as done, so a re-run redoes it.
                frame.to_csv(out_dir / f"failed_chunk_{i:04d}.csv", index=False, encoding="utf-8")
                say(f"CHUNK {i} FAILED CHECKS: {stats}")
                raise
            frame.to_csv(out_dir / f"chunk_{i:04d}.csv", index=False, encoding="utf-8")
            totals.update({k: v for k, v in stats.items() if isinstance(v, int)})
            remaining = len([c for c in range(i + 1, len(chunks) + 1) if c not in done])
            mins = (time.time() - t0) / 60
            say(f"CHUNK {i}/{len(chunks)} done in {mins:.1f} min | companies {stats['companies']} | CH found {stats['ch_found']}"
                f" | domains {stats['domains']} | Hunter lookups {stats['hunter_lookups']} -> emails {stats['emails']}"
                f" | mobiles {stats['phones']} | rows {stats['rows']} | errors {stats['errors'] or 'none'}"
                f" | about {remaining * mins:.0f} min left")
        with httpx.Client(timeout=30) as http:
            bal = balances(http)
        path = combine(out_dir)
        say(f"FINISHED in {(time.time() - started) / 60:.0f} min | totals {dict(totals)} | credits left {bal} | {path}")
        write_status(state="finished", chunks=len(chunks), credits=bal, totals=dict(totals))
    except StopRun as e:
        say(f"STOPPED: {e}")
        write_status(state="stopped", reason=str(e), totals=dict(totals))
        sys.exit(2)
    except Exception as e:
        say(f"CRASHED: {type(e).__name__}: {e}")
        write_status(state="crashed", reason=f"{type(e).__name__}: {e}", totals=dict(totals))
        raise


if __name__ == "__main__":
    main()
