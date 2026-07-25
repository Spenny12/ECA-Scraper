"""Domain discovery via Serper.dev (Google search API)."""
import re
from urllib.parse import urlparse

from config import logger
from models import DomainInfo

EXCLUDED_HOST_FRAGMENTS = ('.gov.uk', '.linkedin.com', 'companieshouse', 'endole', 'yell', 'zoominfo')
IGNORED_NAME_TOKENS = {'limited', 'ltd', 'uk', 'plc', 'group', 'co', 'and'}


async def discover_domains(job, client, company_names, api_key):
    """Guesses each company's website via a Serper search, using simple
    token-overlap heuristics to reject obviously unrelated domains."""
    results, discovered = [], set()
    for name in company_names:
        domain = await _discover_one(client, name, api_key)
        if domain and domain not in discovered:
            results.append(DomainInfo(provided_name=name, discovered_domain=domain))
            discovered.add(domain)
        else:
            results.append(DomainInfo(provided_name=name))
    job.domains_discovered_count = len(discovered)
    return results


async def _discover_one(client, name, api_key) -> str | None:
    search_name = re.sub(r'[^a-zA-Z0-9\s]', '', name).strip()
    payload = {"q": f"{search_name} UK website", "gl": "gb"}
    try:
        res = await client.post("https://google.serper.dev/search", headers={'X-API-KEY': api_key}, json=payload)
    except Exception as e:
        logger.warning(f"Serper lookup failed for '{name}': {e}")
        return None

    if res.status_code != 200:
        logger.warning(f"Serper lookup for '{name}' returned HTTP {res.status_code}")
        return None

    for item in res.json().get('organic', []):
        domain = _match_domain(item, name)
        if domain:
            return domain
    return None


def _match_domain(item: dict, name: str) -> str | None:
    """Returns the item's domain if it plausibly belongs to `name`, else None."""
    netloc = urlparse(item.get('link', '')).netloc.lower()
    if any(fragment in netloc for fragment in EXCLUDED_HOST_FRAGMENTS):
        return None

    raw_dom = netloc.replace('www.', '')
    clean_dom = re.sub(r'[^a-z0-9]', '', raw_dom.split('.')[0])
    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', name.replace('&', ' and ')).lower()
    tokens = [w for w in clean_name.split() if w not in IGNORED_NAME_TOKENS]

    if not tokens:
        return raw_dom

    matches = [t for t in tokens if t in clean_dom]
    if not matches:
        return None
    if any(len(t) >= 4 for t in matches):
        return raw_dom

    # Only short-token matches (e.g. "co", "jd") - require them to make up a
    # meaningful share of the domain name before accepting it.
    matched_chars = sum(len(t) for t in matches)
    for suffix in ('ltd', 'group', 'uk', 'co'):
        if clean_dom.endswith(suffix):
            matched_chars += len(suffix)
    if len(clean_dom) > 0 and matched_chars / len(clean_dom) < 0.4:
        return None
    return raw_dom
