"""Finds email addresses for Companies House directors via Hunter.io's
Email Finder - targeted at a specific named person + domain, rather than
Hunter's Domain Search (which returns whoever it has data on for a domain,
with no guarantee they're actually a director of that company).
"""
from config import logger
from models import ContactDiscoveryResult, EmployeeEmail

EMAIL_FINDER_URL = "https://api.hunter.io/v2/email-finder"


async def discover_director_emails(job, client, company_names, dom_map, ch_map, api_key, max_directors_per_company):
    """For each company with both a discovered domain and Companies House
    directors, looks up each director's email individually (capped at
    max_directors_per_company), so every returned email is checked against
    a specific named director rather than just "someone at this domain."

    Returns {company_name: ContactDiscoveryResult}, containing only
    companies where at least one director's email was actually found.
    """
    results_by_company = {}
    total_found = 0

    for name in company_names:
        domain = dom_map.get(name)
        ch = ch_map.get(name)
        if not domain or not ch or not ch.directors:
            continue  # nothing to search against - no verified domain, or no known directors

        emails = []
        for director in ch.directors[:max_directors_per_company]:
            email = await _find_one(client, domain, director.get("name", ""), api_key)
            if email:
                emails.append(email)

        if emails:
            results_by_company[name] = ContactDiscoveryResult(domain=domain, emails_found=emails)
            total_found += len(emails)

    job.total_emails_found_count = total_found
    return results_by_company


async def _find_one(client, domain, ch_name, api_key):
    first_name, last_name = split_ch_name(ch_name)
    if not first_name or not last_name:
        logger.warning(f"Could not parse director name '{ch_name}' into first/last - skipping Hunter lookup")
        return None

    try:
        # Hunter's API only accepts the key as a query param (no header
        # option), so it will still appear in access logs - that's a
        # limitation of their API, not something fixable here.
        res = await client.get(EMAIL_FINDER_URL, params={
            "domain": domain, "first_name": first_name, "last_name": last_name, "api_key": api_key,
        })
    except Exception as e:
        logger.warning(f"Hunter email-finder failed for '{ch_name}' at {domain}: {e}")
        return None

    if res.status_code != 200:
        logger.warning(f"Hunter email-finder for '{ch_name}' at {domain} returned HTTP {res.status_code}")
        return None

    data = res.json().get("data") or {}
    email = data.get("email")
    if not email:
        return None  # Hunter has no confident match for this specific person - correctly reported as not found

    return EmployeeEmail(
        email=email.lower(),
        type="personal",
        first_name=data.get("first_name") or first_name,
        last_name=data.get("last_name") or last_name,
        position=data.get("position"),
    )


import re

def split_ch_name(ch_name: str):
    """Companies House officer names come as 'SURNAME, First Middle...'.
    Splits that into (first_name, last_name) - used both for Hunter's
    Email Finder (which wants the two separately) and by pipeline.py to
    display director names as "First Last" instead of CH's raw format."""
    if not ch_name:
        return None, None

    # Strip common titles before processing
    ch_name = re.sub(r'^(Mr|Mrs|Miss|Ms|Dr|Sir|Lord)\.?\s+', '', ch_name, flags=re.IGNORECASE)

    if "," in ch_name:
        last, _, rest = ch_name.partition(",")
        rest = rest.strip()
        first = rest.split(" ")[0] if rest else ""
        if not first or not last.strip():
            return None, None
        return first.title(), last.strip().title()
    parts = ch_name.strip().split()
    if len(parts) < 2:
        return None, None
    return parts[0].title(), parts[-1].title()
