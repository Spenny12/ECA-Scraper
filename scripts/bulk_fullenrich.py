"""Runs FullEnrich over an already-exported leads sheet, outside the web app.

For catching up on phone numbers (e.g. after FullEnrich credits ran out
mid-run) without re-running the whole pipeline. Uses the same rules as the
pipeline's FullEnrich step (see fullenrich.py): only rows with a domain match
confidence of at least FULLENRICH_MIN_CONFIDENCE and a named director, phone-only
lookups where an email is already known, and email + phone otherwise. Rows
that already have a phone number are skipped, and the same person at the
same domain is only looked up once.

Usage (from the repo root, with FULLENRICH_API_KEY set in the environment):

    python scripts/bulk_fullenrich.py "leads.csv" --dry-run
    python scripts/bulk_fullenrich.py "leads.csv"

Writes "<input> [FullEnriched].csv" next to the input (or --output), and
rewrites it as each batch finishes. Started batches are recorded in a
".state.json" file next to the output, so if the script is interrupted,
re-running the same command picks the existing batches back up instead of
paying for them again.
"""
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import fullenrich  # noqa: E402

CREDITS_URL = "https://app.fullenrich.com/api/v2/account/credits"
PHONE_CREDITS = 10
EMAIL_CREDITS = 1
COPIED_FIELDS = ("Phone Number", "Employee Email", "Status")


def _confidence(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_eligible(row: dict) -> bool:
    if str(row.get("Phone Number", "")).strip():
        return False
    return fullenrich._is_eligible({**row, "Domain Match Confidence": _confidence(row.get("Domain Match Confidence"))})


def _person_key(row: dict) -> tuple:
    return (
        row["Employee Forename"].strip().lower(),
        row["Employee Surname"].strip().lower(),
        row["Discovered Domain"].strip().lower(),
        fullenrich._has_email(row),
    )


def _group_contacts(rows: list) -> dict:
    """{first row index: [every row index for that person]} for eligible rows."""
    groups = {}
    for i, row in enumerate(rows):
        if _is_eligible(row):
            groups.setdefault(_person_key(row), []).append(i)
    return {indices[0]: indices for indices in groups.values()}


def _load_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"enrichment_ids": [], "started_rows": []}


def _save_state(path: Path, state: dict):
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _write_output(df_columns, rows: list, path: Path):
    pd.DataFrame(rows, columns=df_columns).to_csv(path, index=False, encoding="utf-8")


async def _credit_balance(client, api_key):
    res = await client.get(CREDITS_URL, headers=fullenrich._headers(api_key))
    if res.status_code != 200:
        raise SystemExit(f"FullEnrich credit check failed: HTTP {res.status_code} {res.text[:300]}")
    return res.json().get("balance")


async def run(args):
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_name(f"{input_path.stem} [FullEnriched].csv")
    state_path = output_path.with_name(output_path.name + ".state.json")

    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    if "Phone Number" not in df.columns:
        df["Phone Number"] = ""
    columns = list(df.columns)
    rows = df.to_dict("records")

    state = _load_state(state_path)
    already_started = set(state["started_rows"])
    groups = _group_contacts(rows)
    pending = [i for i in groups if i not in already_started]
    if args.limit:
        pending = pending[:args.limit]

    phone_only = sum(1 for i in pending if fullenrich._has_email(rows[i]))
    with_email = len(pending) - phone_only
    max_credits = phone_only * PHONE_CREDITS + with_email * (PHONE_CREDITS + EMAIL_CREDITS)
    print(f"Rows in sheet:                 {len(rows)}")
    print(f"Eligible rows (no phone yet):  {sum(len(v) for v in groups.values())}")
    print(f"Unique people to look up:      {len(pending)} "
          f"({phone_only} phone only, {with_email} email + phone)")
    if already_started:
        print(f"Already started (resuming):    {len(already_started)} people in {len(state['enrichment_ids'])} batch(es)")
    print(f"Maximum credits if all found:  {max_credits}")
    print(f"Output:                        {output_path}")

    api_key = os.environ.get("FULLENRICH_API_KEY")
    if not api_key:
        raise SystemExit("FULLENRICH_API_KEY is not set in the environment.")

    async with httpx.AsyncClient(timeout=60.0) as client:
        balance = await _credit_balance(client, api_key)
        print(f"FullEnrich credit balance:     {balance}")
        if args.dry_run:
            return
        if pending and not args.yes:
            if input(f"Start {len(pending)} lookup(s)? Type 'yes' to continue: ").strip().lower() != "yes":
                print("Cancelled.")
                return

        # Start every new batch before waiting on any - they run in
        # parallel on FullEnrich's side. Each is saved to the state file as
        # soon as it starts, so an interruption never loses a paid batch.
        contacts = [fullenrich._to_contact(i, rows[i]) for i in pending]
        batches = [contacts[i:i + fullenrich.BATCH_SIZE] for i in range(0, len(contacts), fullenrich.BATCH_SIZE)]
        for n, batch in enumerate(batches, start=1):
            enrichment_id = await fullenrich._start_batch(client, api_key, f"{input_path.stem} ({n}/{len(batches)})", batch)
            if not enrichment_id:
                print(f"Batch {n}/{len(batches)} failed to start - re-run later to retry it.")
                continue
            state["enrichment_ids"].append(enrichment_id)
            state["started_rows"].extend(int(c["custom"]["row"]) for c in batch)
            _save_state(state_path, state)
            print(f"Started batch {n}/{len(batches)}: {enrichment_id}")

        # Fetching results again for batches finished on a previous run is
        # free, so every recorded batch is (re)applied to the fresh sheet.
        job = SimpleNamespace(id=input_path.stem, total_emails_found_count=0)
        phones_found = 0
        for n, enrichment_id in enumerate(state["enrichment_ids"], start=1):
            print(f"Waiting for batch {n}/{len(state['enrichment_ids'])} ({enrichment_id})...")
            for record in await fullenrich._wait_for_results(client, api_key, enrichment_id):
                fullenrich._apply_result(job, rows, record)
                try:
                    first = int((record.get("custom") or {}).get("row"))
                except (TypeError, ValueError):
                    continue
                if rows[first].get("Phone Number"):
                    phones_found += 1
                for i in groups.get(first, [first])[1:]:
                    for field in COPIED_FIELDS:
                        rows[i][field] = rows[first][field]
            _write_output(columns, rows, output_path)

        print(f"Done. Phones found: {phones_found}, new emails found: {job.total_emails_found_count}")
        print(f"Credits left: {await _credit_balance(client, api_key)}")
        print(f"Saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Run FullEnrich over an exported leads sheet.")
    parser.add_argument("input", help="CSV exported from the tool")
    parser.add_argument("--output", help="where to write the enriched CSV (default: next to the input)")
    parser.add_argument("--dry-run", action="store_true", help="show counts and credit cost, then stop")
    parser.add_argument("--limit", type=int, help="only look up this many people (for a small test run)")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
