"""Companies House lookups: registered address + active directors."""
from urllib.parse import quote_plus

from config import logger
from models import CHCompanyData

BASE_URL = "https://api.company-information.service.gov.uk"
ACTIVE_DIRECTOR_ROLES = ("director", "nominee-director")


async def enrich_companies(job, client, company_names, company_metadata, api_key):
    """Looks up each company on Companies House.

    Uses the exact company number when we already know it (e.g. from a Land
    Registry export's "Company Registration No." column) rather than a
    fuzzy name search, since common company names can collide and a name
    search can silently return the wrong company.
    """
    auth = (api_key, "")
    results = []
    for name in company_names:
        known_number = (company_metadata.get(name) or {}).get('company_number')
        results.append(await _enrich_one(client, auth, name, known_number))
    return results


async def _enrich_one(client, auth, name, known_number) -> CHCompanyData:
    info = CHCompanyData(company_name=name)
    try:
        if known_number:
            await _lookup_by_number(client, auth, info, name, known_number)
        if not info.company_number:
            await _lookup_by_name(client, auth, info, name)
        if info.company_number:
            info.directors = await _fetch_active_directors(client, auth, info.company_number)
    except Exception as e:
        logger.warning(f"Companies House lookup failed for '{name}': {e}")
    return info


async def _lookup_by_number(client, auth, info: CHCompanyData, name: str, known_number: str):
    res = await client.get(f"{BASE_URL}/company/{quote_plus(known_number)}", auth=auth)
    if res.status_code != 200:
        logger.warning(
            f"CH direct lookup for '{name}' (no. {known_number}) returned HTTP {res.status_code}; "
            f"falling back to name search"
        )
        return
    profile = res.json()
    info.company_number = profile.get("company_number", known_number)
    info.registered_address = _format_address(profile.get("registered_office_address", {}))


async def _lookup_by_name(client, auth, info: CHCompanyData, name: str):
    res = await client.get(f"{BASE_URL}/search/companies?q={quote_plus(name)}", auth=auth)
    if res.status_code != 200:
        logger.warning(f"CH name search for '{name}' returned HTTP {res.status_code}")
        return
    items = res.json().get("items")
    if not items:
        return
    top = items[0]
    info.company_number = top.get("company_number")
    info.registered_address = _format_address(top.get("address", {}))


async def _fetch_active_directors(client, auth, company_number: str) -> list:
    res = await client.get(f"{BASE_URL}/company/{company_number}/officers", auth=auth)
    if res.status_code != 200:
        logger.warning(f"CH officers lookup for {company_number} returned HTTP {res.status_code}")
        return []
    return [
        {"name": o.get("name"), "role": o.get("officer_role", "Director").title()}
        for o in res.json().get("items", [])
        if o.get("officer_role") in ACTIVE_DIRECTOR_ROLES and not o.get("resigned_on")
    ]


def _format_address(addr: dict) -> str:
    return ", ".join(addr.get(k) for k in ("address_line_1", "locality", "postal_code") if addr.get(k))
