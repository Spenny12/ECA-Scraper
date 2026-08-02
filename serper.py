"""Domain discovery via Serper.dev (Google search API)."""
import re
from difflib import SequenceMatcher
from urllib.parse import urlparse

from config import logger
from models import DomainInfo

EXCLUDED_HOST_FRAGMENTS = ('.gov.uk', '.linkedin.com', 'companieshouse', 'endole', 'yell', 'zoominfo')
IGNORED_NAME_TOKENS = {'limited', 'ltd', 'uk', 'plc', 'group', 'co', 'and'}

# Minimum whole-string similarity between the company name and a candidate
# domain before we accept it as a match. Below this, we return "not found"
# rather than risk attaching the wrong company's website (and therefore
# emailing the wrong people).
#
# This replaced an earlier "does any single word appear somewhere in the
# domain" check, which produced false positives like matching
# "JEG PROPERTY LETS LTD" to "vi-lets.com" purely because "lets" happens to
# be a substring of "vi-lets" - the match was coincidental, not meaningful.
MATCH_CONFIDENCE_THRESHOLD = 0.5


async def discover_domains(job, client, company_names, api_key):
    """Guesses each company's website via a Serper search, using a
    whole-string similarity score to reject unrelated domains."""
    results, discovered = [], set()
    for name in company_names:
        domain, confidence = await _discover_one(client, name, api_key)
        if domain and domain not in discovered:
            results.append(DomainInfo(provided_name=name, discovered_domain=domain, match_confidence=confidence))
            discovered.add(domain)
        else:
            results.append(DomainInfo(provided_name=name))
    job.domains_discovered_count = len(discovered)
    return results


async def _discover_one(client, name, api_key) -> tuple[str | None, float | None]:
    search_name = re.sub(r'[^a-zA-Z0-9\s]', '', name).strip()
    payload = {"q": f"{search_name} UK website", "gl": "gb"}
    try:
        res = await client.post("https://google.serper.dev/search", headers={'X-API-KEY': api_key}, json=payload)
    except Exception as e:
        logger.warning(f"Serper lookup failed for '{name}': {e}")
        return None, None

    if res.status_code != 200:
        logger.warning(f"Serper lookup for '{name}' returned HTTP {res.status_code}")
        return None, None

    for item in res.json().get('organic', []):
        domain, confidence = _match_domain(item, name)
        if domain:
            return domain, confidence
    return None, None


def _match_domain(item: dict, name: str) -> tuple[str | None, float | None]:
    """Returns (domain, confidence) if the item plausibly belongs to `name`,
    else (None, None)."""
    netloc = urlparse(item.get('link', '')).netloc.lower()
    if any(fragment in netloc for fragment in EXCLUDED_HOST_FRAGMENTS):
        return None, None

    raw_dom = netloc.replace('www.', '')
    clean_dom = re.sub(r'[^a-z0-9]', '', raw_dom.split('.')[0])
    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', name.replace('&', ' and ')).lower()
    tokens = [w for w in clean_name.split() if w not in IGNORED_NAME_TOKENS]

    if not tokens or not clean_dom:
        return None, None

    confidence = _similarity(''.join(tokens), clean_dom)
    if confidence < MATCH_CONFIDENCE_THRESHOLD:
        return None, None
    return raw_dom, round(confidence, 2)


def _similarity(name_str: str, domain_str: str) -> float:
    """Whole-string similarity (0-1) between the company name (filler words
    stripped) and the candidate domain. Uses difflib's ratio, which credits
    overall structural overlap rather than any single matching substring -
    so a company name that shares several word-fragments with a domain
    scores higher than one that coincidentally shares one unrelated word."""
    return SequenceMatcher(None, name_str, domain_str).ratio()
