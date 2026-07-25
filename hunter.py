"""Employee email discovery via Hunter.io, filtered to named (non-generic) inboxes."""
from config import logger
from models import ContactDiscoveryResult, EmployeeEmail

GENERIC_PREFIXES = ('info@', 'admin@', 'customercare@', 'customer.service@', 'sales@', 'support@', 'contact@')


async def discover_contacts(job, client, discovery_results, api_key, max_per_domain):
    domains = [r.discovered_domain for r in discovery_results if r.discovered_domain]
    results = [await _discover_one(client, domain, api_key, max_per_domain) for domain in domains]
    job.total_emails_found_count = sum(len(r.emails_found) for r in results)
    return results


async def _discover_one(client, domain, api_key, max_per_domain) -> ContactDiscoveryResult:
    res_obj = ContactDiscoveryResult(domain=domain)
    try:
        # Hunter's API only accepts the key as a query param (no header
        # option), so it will still appear in access logs - that's a
        # limitation of their API, not something fixable here.
        res = await client.get(
            "https://api.hunter.io/v2/domain-search",
            params={"domain": domain, "limit": max_per_domain, "api_key": api_key},
        )
    except Exception as e:
        logger.warning(f"Hunter lookup failed for '{domain}': {e}")
        return res_obj

    if res.status_code != 200:
        logger.warning(f"Hunter lookup for '{domain}' returned HTTP {res.status_code}")
        return res_obj

    res_obj.emails_found = [
        EmployeeEmail(
            email=e.get('value', '').lower(), type=e.get('type', 'personal'),
            first_name=e.get('first_name'), last_name=e.get('last_name'), position=e.get('position'),
        )
        for e in res.json().get('data', {}).get('emails', [])
        if not any(e.get('value', '').lower().startswith(p) for p in GENERIC_PREFIXES)
    ]
    return res_obj
