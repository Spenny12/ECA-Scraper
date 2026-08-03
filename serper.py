"""Domain discovery via Serper.dev (Google search API).

Filters unrelated/foreign domains using three independent signals, since a
plain ".com filter" is far too crude (UK companies commonly use .com, and a
foreign company's .com is indistinguishable from a UK one by TLD alone):

1. Name similarity - whole-string comparison between the company name and
   the candidate domain (catches coincidental substring matches, see the
   "vi-lets.com" / "JEG PROPERTY LETS LTD" case).
2. A hard blocklist of non-UK country-code TLDs (.in, .de, .us, etc.) -
   catches obviously-foreign domains regardless of how well the name matches.
3. A locality bonus - when we know the company's registered town from
   Companies House, we bias the Google search toward that location AND
   reward results whose title/snippet mention it. This is what catches
   cases like "JEET CORPORATION LTD" matching an identically-named but
   unrelated Indian company on a .com domain - name similarity alone can't
   tell the two apart, but "Rotherham" (or whatever the real UK town is)
   appearing in the genuine company's search result can.

Rather than accepting the first result that clears the threshold, all
organic results are scored and the best one wins - so a correct, locality-
matched result outranks an earlier but unrelated same-name result.
"""
import re
from difflib import SequenceMatcher
from urllib.parse import urlparse

from config import logger
from models import DomainInfo

EXCLUDED_HOST_FRAGMENTS = ('.gov.uk', '.linkedin.com', 'companieshouse', 'endole', 'yell', 'zoominfo', '.gov', '.ac', '.nhs.uk', '.police.uk')
IGNORED_NAME_TOKENS = {'limited', 'ltd', 'uk', 'plc', 'group', 'co', 'and'}

# Country-code TLDs that indicate the site is very unlikely to be a UK
# company's own domain. Not exhaustive (an allowlist would be impossible -
# UK companies legitimately use .com/.net/.org/.io as often as .co.uk) but
# catches the common cases where a same-named foreign company's site would
# otherwise slip through on name similarity alone.
NON_UK_CCTLDS = {
    'in', 'us', 'de', 'fr', 'es', 'it', 'nl', 'au', 'ca', 'nz', 'sg', 'ae',
    'za', 'br', 'mx', 'ru', 'cn', 'jp', 'pk', 'bd', 'ng', 'ie', 'pl', 'se',
    'no', 'dk', 'fi', 'ch', 'at', 'be', 'pt', 'gr', 'tr', 'kr', 'id', 'my',
    'ph', 'vn', 'th', 'hk', 'tw', 'sa', 'eg', 'ar', 'cl', 'co',  # .co (Colombia's cc-TLD; distinct from generic "co" token matching)
}
UK_TLD_SUFFIXES = ('.co.uk', '.org.uk', '.ltd.uk', '.plc.uk', '.me.uk', '.net.uk', '.uk')

NAME_SIMILARITY_WEIGHT = 0.7
LOCALITY_BONUS = 0.2
UK_TLD_BONUS = 0.1
MATCH_CONFIDENCE_THRESHOLD = 0.65


async def discover_domains(job, client, company_names, api_key, locality_map=None):
    """Guesses each company's website via a Serper search.

    locality_map (optional): {company_name: registered_locality}, from
    Companies House. When present for a company, it's used both to bias the
    Google search itself (Serper's `location` param) and to score results
    (a title/snippet mentioning the locality scores higher).
    """
    locality_map = locality_map or {}
    results, discovered = [], set()
    for name in company_names:
        locality = locality_map.get(name)
        domain, confidence = await _discover_one(client, name, api_key, locality)
        if domain and domain not in discovered:
            results.append(DomainInfo(provided_name=name, discovered_domain=domain, match_confidence=confidence))
            discovered.add(domain)
        else:
            results.append(DomainInfo(provided_name=name))
    job.domains_discovered_count = len(discovered)
    return results


async def _discover_one(client, name, api_key, locality: str | None) -> tuple[str | None, float | None]:
    search_name = re.sub(r'[^a-zA-Z0-9\s]', '', name).strip()
    query = f"{search_name} {locality} UK company website" if locality else f"{search_name} UK website"
    payload = {"q": query, "gl": "gb"}
    if locality:
        # City-level location bias, per Serper's docs - stronger signal than
        # the country-level `gl` alone.
        payload["location"] = f"{locality}, United Kingdom"

    try:
        res = await client.post("https://google.serper.dev/search", headers={'X-API-KEY': api_key}, json=payload)
    except Exception as e:
        logger.warning(f"Serper lookup failed for '{name}': {e}")
        return None, None

    if res.status_code != 200:
        logger.warning(f"Serper lookup for '{name}' returned HTTP {res.status_code}")
        return None, None

    best_domain, best_score = None, 0.0
    for item in res.json().get('organic', []):
        domain, score = _score_candidate(item, name, locality)
        if domain and score > best_score:
            best_domain, best_score = domain, score

    if best_domain and best_score >= MATCH_CONFIDENCE_THRESHOLD:
        return best_domain, round(min(best_score, 1.0), 2)
    return None, None


def _score_candidate(item: dict, name: str, locality: str | None) -> tuple[str | None, float]:
    """Returns (domain, score) for one Serper organic result, or (None, 0)
    if it's disqualified outright (wrong TLD, excluded host, no name overlap)."""
    netloc = urlparse(item.get('link', '')).netloc.lower()
    if any(fragment in netloc for fragment in EXCLUDED_HOST_FRAGMENTS):
        return None, 0.0

    raw_dom = netloc.replace('www.', '')
    if _tld_of(raw_dom) in NON_UK_CCTLDS:
        return None, 0.0  # hard reject - not a .com/.uk ambiguity, an explicit foreign ccTLD

    clean_dom = re.sub(r'[^a-z0-9]', '', raw_dom.split('.')[0])
    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', name.replace('&', ' and ')).lower()
    tokens = [w for w in clean_name.split() if w not in IGNORED_NAME_TOKENS]
    if not tokens or not clean_dom:
        return None, 0.0

    # Require that at least one distinct part of the name is in the domain
    valid_match = False
    for t in tokens:
        if len(t) > 2 and t in clean_dom:
            valid_match = True
            break

    if not valid_match:
        return None, 0.0

    similarity = SequenceMatcher(None, ''.join(tokens), clean_dom).ratio()

    # Penalize if the token string is very short
    if len(''.join(tokens)) < 5:
        similarity *= 0.5

    score = similarity * NAME_SIMILARITY_WEIGHT

    if raw_dom.endswith(UK_TLD_SUFFIXES):
        score += UK_TLD_BONUS

    if locality:
        haystack = f"{item.get('title', '')} {item.get('snippet', '')}".lower()
        if locality.lower() in haystack:
            score += LOCALITY_BONUS

    return raw_dom, score


def _tld_of(domain: str) -> str:
    parts = domain.rstrip('/').split('.')
    return parts[-1] if parts else ''
