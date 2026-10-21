"""Phone (and fallback email) enrichment via FullEnrich's bulk API (v2).

Only rows whose discovered domain is a confident match (at least
FULLENRICH_MIN_CONFIDENCE) and that name a specific person are sent, since
FullEnrich charges per result (1 credit per work email, 10 per mobile).
Rows that already have a Hunter email only ask for a phone number - Hunter's
email is kept and FullEnrich isn't asked to find another. A company with
several properties has a copy of each contact row per property, so each
person is only looked up once and the result is copied to all their rows.

FullEnrich is asynchronous: each batch of up to 100 contacts is submitted,
then polled until it finishes. Results are matched back to rows via the
`custom` passthrough field.
"""
import asyncio

from config import FULLENRICH_MIN_CONFIDENCE, logger

BASE_URL = "https://app.fullenrich.com/api/v2/contact/enrich/bulk"
BATCH_SIZE = 100            # FullEnrich's max contacts per request
POLL_INTERVAL_SECONDS = 30  # FullEnrich's own "try again in 30 seconds"
MAX_WAIT_SECONDS = 60 * 60  # give up on a batch after an hour
TERMINAL_STATUSES = ("FINISHED", "CANCELED", "CREDITS_INSUFFICIENT", "UNKNOWN")
ACCEPTED_EMAIL_STATUSES = ("DELIVERABLE", "HIGH_PROBABILITY")
# Fields _apply_result can fill, copied from a person's looked-up row to
# their other rows.
ENRICHED_FIELDS = ("Phone Number", "Employee Email", "Status")


def _headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def _has_email(row: dict) -> bool:
    return "@" in str(row.get("Employee Email", ""))


def _is_eligible(row: dict) -> bool:
    confidence = row.get("Domain Match Confidence")
    return (
        confidence is not None and confidence >= FULLENRICH_MIN_CONFIDENCE
        and row.get("Employee Forename") and row.get("Employee Surname")
        and row.get("Employee Name") != "UNIDENTIFIED"
    )


def group_by_person(rows: list, indices) -> dict:
    """{first row index: [every row index for that person]} - one lookup
    per person at a domain, whatever number of rows they appear on."""
    groups = {}
    for i in indices:
        row = rows[i]
        key = (
            str(row["Employee Forename"]).strip().lower(),
            str(row["Employee Surname"]).strip().lower(),
            str(row["Discovered Domain"]).strip().lower(),
            _has_email(row),
        )
        groups.setdefault(key, []).append(i)
    return {indices[0]: indices for indices in groups.values()}


def copy_to_group(rows: list, groups: dict, first: int):
    for i in groups.get(first, [first])[1:]:
        for field in ENRICHED_FIELDS:
            if field in rows[first]:
                rows[i][field] = rows[first][field]


def _to_contact(index: int, row: dict) -> dict:
    enrich_fields = ["contact.phones"] if _has_email(row) else ["contact.work_emails", "contact.phones"]
    return {
        "first_name": row["Employee Forename"],
        "last_name": row["Employee Surname"],
        "domain": row["Discovered Domain"],
        "company_name": row["Target Company Name"],
        "enrich_fields": enrich_fields,
        "custom": {"row": str(index)},
    }


async def enrich_rows(job, client, rows: list, api_key: str):
    """Fills in "Phone Number" (and "Employee Email" where Hunter found
    none) on eligible rows, in place."""
    groups = group_by_person(rows, [i for i, row in enumerate(rows) if _is_eligible(row)])
    contacts = [_to_contact(i, rows[i]) for i in groups]
    if not contacts:
        return
    logger.info(f"FullEnrich: enriching {len(contacts)} contact(s) for job {job.id}")

    batches = [contacts[i:i + BATCH_SIZE] for i in range(0, len(contacts), BATCH_SIZE)]
    enrichment_ids = []
    for n, batch in enumerate(batches, start=1):
        enrichment_id = await _start_batch(client, api_key, f"{job.id} ({n}/{len(batches)})", batch)
        if enrichment_id:
            enrichment_ids.append(enrichment_id)

    # Batches run concurrently on FullEnrich's side, so they're all started
    # before waiting on any of them.
    for enrichment_id in enrichment_ids:
        for record in await _wait_for_results(client, api_key, enrichment_id):
            first = _apply_result(job, rows, record)
            if first is not None:
                copy_to_group(rows, groups, first)


async def _start_batch(client, api_key, name, batch):
    payload = {"name": name, "data": batch}
    for attempt in range(6):
        try:
            res = await client.post(BASE_URL, params={"silentFail": "true"}, headers=_headers(api_key), json=payload)
        except Exception as e:
            logger.warning(f"FullEnrich start failed for '{name}': {e}")
            return None
        if res.status_code == 429:
            logger.warning(f"FullEnrich rate limit hit starting '{name}'. Pausing 60s (attempt {attempt + 1}/6)...")
            await asyncio.sleep(60)
            continue
        if res.status_code != 200:
            logger.warning(f"FullEnrich start for '{name}' returned HTTP {res.status_code}: {res.text[:300]}")
            return None
        return res.json().get("enrichment_id")
    return None


async def _wait_for_results(client, api_key, enrichment_id) -> list:
    waited = 0
    while waited < MAX_WAIT_SECONDS:
        try:
            res = await client.get(f"{BASE_URL}/{enrichment_id}", headers=_headers(api_key))
        except Exception as e:
            logger.warning(f"FullEnrich poll failed for {enrichment_id}: {e}")
            res = None

        if res is not None and res.status_code in (200, 402):
            body = res.json()
            status = body.get("status")
            if status in TERMINAL_STATUSES:
                if status != "FINISHED":
                    logger.warning(f"FullEnrich enrichment {enrichment_id} ended with status {status}")
                return body.get("data") or []
        elif res is not None and res.status_code not in (400, 429):
            # 400 is "not ready yet" and 429 is a rate limit - both just
            # mean wait and retry. Anything else won't fix itself.
            logger.warning(f"FullEnrich poll for {enrichment_id} returned HTTP {res.status_code}: {res.text[:300]}")
            return []

        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        waited += POLL_INTERVAL_SECONDS

    logger.warning(f"FullEnrich enrichment {enrichment_id} didn't finish within {MAX_WAIT_SECONDS}s; skipping it")
    return []


def _apply_result(job, rows: list, record: dict):
    """Fills in one looked-up row; returns its index, or None if the record
    can't be matched to a row."""
    try:
        index = int((record.get("custom") or {}).get("row"))
        row = rows[index]
    except (TypeError, ValueError, IndexError):
        return None
    info = record.get("contact_info") or {}

    phone = (info.get("most_probable_phone") or {}).get("number")
    if phone:
        row["Phone Number"] = phone

    if not _has_email(row):
        work_email = info.get("most_probable_work_email") or {}
        if work_email.get("email") and work_email.get("status") in ACCEPTED_EMAIL_STATUSES:
            row["Employee Email"] = work_email["email"].lower()
            row["Status"] = "Complete (Email Found)"
            job.total_emails_found_count += 1
    return index
