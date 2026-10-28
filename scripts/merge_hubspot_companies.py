"""Merges duplicate HubSpot companies that share a Companies House number.

Reads the "companies to merge" CSV (one row per duplicate: the record to keep
and the record to merge into it) and merges each pair through HubSpot's API.
The kept record's values win where both have one - including the company
owner - so decide owners before running, or fix them afterwards.

Usage (from the repo root, with HUBSPOT_ACCESS_TOKEN set in the environment):

    python scripts/merge_hubspot_companies.py "merge list.csv" --dry-run
    python scripts/merge_hubspot_companies.py "merge list.csv"

Every merge is recorded in "<input> (merge results).csv" as it happens, so if
the script is interrupted, re-running the same command skips the merges that
already went through. The results also give each kept company's final Record
ID, in case HubSpot assigns a new one when merging.
"""
import argparse
import csv
import os
import sys
import time
from pathlib import Path

import httpx

BASE_URL = "https://api.hubapi.com"
REQUEST_INTERVAL = 0.15   # stay well under HubSpot's request limits
KEEP_COL, MERGE_COL = "Keep: Record ID", "Merge into it: Record ID"
RESULT_FIELDS = ["Companies House Number", KEEP_COL, MERGE_COL, "Result", "Final Record ID", "Detail"]


def _read_rows(path: Path) -> list:
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if rows and (KEEP_COL not in rows[0] or MERGE_COL not in rows[0]):
        raise SystemExit(f"Expected columns '{KEEP_COL}' and '{MERGE_COL}' in {path.name}")
    return [r for r in rows if r[KEEP_COL].strip() and r[MERGE_COL].strip()]


def _done_pairs(results_path: Path) -> dict:
    """{(keep id, merge id): final id} for merges that already succeeded."""
    if not results_path.exists():
        return {}
    with open(results_path, encoding="utf-8", newline="") as f:
        return {(r[KEEP_COL], r[MERGE_COL]): r["Final Record ID"] for r in csv.DictReader(f) if r["Result"] in ("merged", "already merged")}


def _merge(client: httpx.Client, token: str, keep_id: str, merge_id: str) -> tuple:
    """Returns (result, final record id, detail)."""
    for attempt in range(5):
        time.sleep(REQUEST_INTERVAL)
        res = client.post(f"{BASE_URL}/crm/v3/objects/companies/merge",
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                          json={"primaryObjectId": keep_id, "objectIdToMerge": merge_id})
        if res.status_code == 429:
            time.sleep(float(res.headers.get("Retry-After", 10)))
            continue
        if res.status_code in (200, 201):
            return "merged", str(res.json().get("id", keep_id)), ""
        if res.status_code == 404:
            # Usually means the duplicate was already merged (by hand or a previous run).
            return "already merged", keep_id, res.text[:200]
        return "failed", keep_id, f"HTTP {res.status_code}: {res.text[:300]}"
    return "failed", keep_id, "still rate limited after retries"


def run(args, client: httpx.Client = None):
    path = Path(args.input)
    results_path = path.with_name(path.stem + " (merge results).csv")
    rows = _read_rows(path)
    done = _done_pairs(results_path)
    todo = [r for r in rows if (r[KEEP_COL].strip(), r[MERGE_COL].strip()) not in done]
    owners = sum(1 for r in todo if r.get("Different owners?", "").strip())
    print(f"Merges in file:            {len(rows)}")
    print(f"Already done (skipping):   {len(rows) - len(todo)}")
    print(f"To merge now:              {len(todo)} into {len({r[KEEP_COL] for r in todo})} companies")
    print(f"With two different owners: {owners} (the kept record's owner wins)")
    if args.dry_run or not todo:
        return

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        raise SystemExit("HUBSPOT_ACCESS_TOKEN is not set in the environment.")
    if not args.yes and input(f"Merge {len(todo)} companies in HubSpot? This can't be undone. Type 'yes': ").strip().lower() != "yes":
        print("Cancelled.")
        return

    new_file = not results_path.exists()
    own_client = client is None
    client = client or httpx.Client(timeout=60.0)
    # If HubSpot gives a kept company a new ID, later merges into it must use that ID.
    current_id = {keep: final for (keep, _), final in done.items()}
    counts = {"merged": 0, "already merged": 0, "failed": 0}
    try:
        with open(results_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
            if new_file:
                writer.writeheader()
            for n, r in enumerate(todo, start=1):
                keep, merge = r[KEEP_COL].strip(), r[MERGE_COL].strip()
                result, final, detail = _merge(client, token, current_id.get(keep, keep), merge)
                if result != "failed":
                    current_id[keep] = final
                counts[result] += 1
                writer.writerow({"Companies House Number": r.get("Companies House Number", ""), KEEP_COL: keep, MERGE_COL: merge,
                                 "Result": result, "Final Record ID": final, "Detail": detail})
                f.flush()
                if result == "failed":
                    print(f"  {n}/{len(todo)} FAILED {merge} -> {keep}: {detail}")
                elif n % 25 == 0:
                    print(f"  {n}/{len(todo)} done")
    finally:
        if own_client:
            client.close()
    print(f"Done. Merged: {counts['merged']}, already merged: {counts['already merged']}, failed: {counts['failed']}")
    changed = {k: v for k, v in current_id.items() if v != k}
    if changed:
        print(f"{len(changed)} kept compan{'y' if len(changed) == 1 else 'ies'} got a new Record ID - see 'Final Record ID' in the results file.")
    print(f"Results saved to {results_path}")


def main():
    parser = argparse.ArgumentParser(description="Merge duplicate HubSpot companies from a merge list.")
    parser.add_argument("input", help="the 'companies to merge' CSV")
    parser.add_argument("--dry-run", action="store_true", help="show what would be merged, then stop")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
