"""Pushes a completed pipeline job's results into HubSpot as Contacts.

Requires a HubSpot private app (Settings -> Integrations -> Private Apps)
with these scopes:
  - crm.objects.contacts (read + write)
  - crm.schemas.contacts (read + write)   - only needed to auto-create the
    custom properties below; skip it if you'd rather create them once by
    hand in the HubSpot UI.

Contacts are upserted keyed on email via HubSpot's batch upsert endpoint, so
re-running a sync after a re-enrichment just updates the same records rather
than creating duplicates.

Custom properties (prefixed `eca_` to avoid clashing with anything else in
the portal): eca_property_address, eca_purchase_date, eca_company_number,
eca_registered_address, eca_discovered_domain, eca_lead_status.
"""
import httpx

from config import HUBSPOT_ACCESS_TOKEN, logger

BASE_URL = "https://api.hubapi.com"
BATCH_SIZE = 100  # HubSpot's batch upsert max per request

CUSTOM_PROPERTIES = [
    {"name": "eca_property_address", "label": "ECA Property Address", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
    {"name": "eca_purchase_date", "label": "ECA Purchase Date", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
    {"name": "eca_company_number", "label": "ECA Companies House Number", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
    {"name": "eca_registered_address", "label": "ECA Registered Address", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
    {"name": "eca_discovered_domain", "label": "ECA Discovered Domain", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
    {"name": "eca_lead_status", "label": "ECA Lead Status", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
]


def _headers() -> dict:
    return {"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}", "Content-Type": "application/json"}


async def ensure_custom_properties(client: httpx.AsyncClient):
    """Creates the custom contact properties if they don't already exist.

    Safe to call on every sync - it's a cheap existence check per property,
    and HubSpot property names are immutable/unique so there's no risk of
    duplicating them.
    """
    for prop in CUSTOM_PROPERTIES:
        res = await client.get(f"{BASE_URL}/crm/v3/properties/contacts/{prop['name']}", headers=_headers())
        if res.status_code == 200:
            continue
        create_res = await client.post(f"{BASE_URL}/crm/v3/properties/contacts", headers=_headers(), json=prop)
        if create_res.status_code not in (200, 201):
            logger.warning(f"Could not create HubSpot property '{prop['name']}': HTTP {create_res.status_code} {create_res.text[:300]}")


def _row_to_contact(row: dict) -> dict | None:
    """Maps one combine_results() row to HubSpot contact properties.
    Returns None for rows with no real email (nothing to sync)."""
    email = str(row.get("Employee Email", "")).strip().lower()
    if not email or email == "nan" or "none found" in email:
        return None

    name = str(row.get("Employee Name", "")).strip()
    if name and name != "UNIDENTIFIED":
        parts = name.split(" ", 1)
        first_name, last_name = parts[0], (parts[1] if len(parts) > 1 else "")
    else:
        first_name, last_name = "", ""

    contact = {
        "email": email,
        "firstname": first_name,
        "lastname": last_name,
        "company": row.get("Target Company Name", ""),
        "eca_property_address": row.get("Property Address", ""),
        "eca_purchase_date": str(row.get("Purchase Date", "")),
        "eca_company_number": row.get("Companies House Number", ""),
        "eca_registered_address": row.get("Registered Address", ""),
        "eca_discovered_domain": row.get("Discovered Domain", ""),
        "eca_lead_status": row.get("Status", ""),
    }
    # Only sent when present - an empty value would wipe a phone number
    # already on the HubSpot contact.
    if row.get("Phone Number"):
        contact["phone"] = row["Phone Number"]
    return contact


async def sync_results_to_hubspot(results: list) -> dict:
    """Upserts every contact-bearing row in `results` into HubSpot, keyed on
    email. Returns {"synced": n, "skipped": n, "errors": [...]}."""
    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("HUBSPOT_ACCESS_TOKEN is not configured.")

    contacts = [c for c in (_row_to_contact(r) for r in results) if c]
    summary = {"synced": 0, "skipped": len(results) - len(contacts), "errors": []}
    if not contacts:
        return summary

    async with httpx.AsyncClient(timeout=30.0) as client:
        await ensure_custom_properties(client)
        for i in range(0, len(contacts), BATCH_SIZE):
            batch = contacts[i:i + BATCH_SIZE]
            payload = {"inputs": [{"idProperty": "email", "id": c["email"], "properties": c} for c in batch]}
            res = await client.post(f"{BASE_URL}/crm/v3/objects/contacts/batch/upsert", headers=_headers(), json=payload)
            if res.status_code in (200, 201):
                summary["synced"] += len(batch)
            else:
                logger.warning(f"HubSpot batch upsert failed: HTTP {res.status_code} {res.text[:300]}")
                summary["errors"].append(f"Batch starting at row {i}: HTTP {res.status_code} - {res.text[:300]}")

    return summary
