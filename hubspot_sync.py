"""Pushes a completed pipeline job's results into HubSpot as Companies,
Contacts and Projects (a custom object with its own pipeline).

Create-only: nothing that already exists in HubSpot is ever updated. Each run
loads what's already there and only creates what's genuinely new:

- Companies are matched to existing ones by name (ignoring punctuation and
  LTD/LIMITED). A matched company is reused as-is - never renamed - so new
  people at a company the team already owns get linked to it and inherit its
  owner via HubSpot's owner-assignment workflows.
- Contacts are matched by email, or by phone for contacts without one. An
  existing contact is left completely untouched (no field updates and no new
  company link), so nobody's contacts get moved or reassigned.
- Projects are matched by property address and skipped if already there.
- Contacts with neither an email nor a phone number are never sent, and a
  company with no such contact is skipped entirely, along with its projects.

The team can't create HubSpot properties, so this only writes to properties
that already exist. They're found by their labels in HubSpot at runtime (see
PROPERTY_LABELS) rather than hard-coded internal names.

Requires a HubSpot private app ("Development -> Legacy Apps" in HubSpot) with
scopes crm.objects.{contacts,companies,custom}.{read,write} and
crm.schemas.{contacts,companies,custom}.read, its token set as the
HUBSPOT_ACCESS_TOKEN environment variable.
"""
import asyncio
import re
import time

import httpx

from config import HUBSPOT_ACCESS_TOKEN, HUBSPOT_PROJECT_OBJECT, logger

BASE_URL = "https://api.hubapi.com"
BATCH_SIZE = 100            # HubSpot's max inputs per batch request
REQUEST_INTERVAL = 0.12     # ~8 requests/second, under private-app limits
SEARCH_INTERVAL = 0.26      # the search API allows ~4 requests/second

PROJECT_LABEL = "project"
PIPELINE_LABEL = "Project Pipeline"
STAGE_LABEL = "Planning"

# Export column -> HubSpot property label, per object. Labels are looked up
# in HubSpot to get each property's internal name and type.
PROPERTY_LABELS = {
    "companies": {
        "Target Company Name": "Company name",
        "Discovered Domain": "Company Domain Name",
    },
    "contacts": {
        "Employee Forename": "First Name",
        "Employee Surname": "Last Name",
        "Employee Position": "Job Title",
        "Employee Email": "Email",
        "Phone Number": "Phone Number",
        "Companies House Number": "Companies House Number",
        "Registered Address": "Registered Address",
        "Domain Match Confidence": "Domain Match Confidence",
    },
    "projects": {
        "Property Address": "Property Address",
        "Purchase Date": "Purchase Date",
        "Price Paid": "Amount paid",
    },
}
COMPANY_SUFFIX_RE = re.compile(r"(LIMITED|LTD|PLC|LLP)$")


class HubSpotError(Exception):
    pass


class _Client:
    """httpx wrapper adding auth, request pacing and 429 retries."""

    def __init__(self, client: httpx.AsyncClient, token: str):
        self.client = client
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.last = {"default": 0.0, "search": 0.0}

    async def request(self, method: str, path: str, json=None, params=None, kind: str = "default"):
        interval = SEARCH_INTERVAL if kind == "search" else REQUEST_INTERVAL
        for attempt in range(5):
            wait = self.last[kind] + interval - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self.last[kind] = time.monotonic()
            res = await self.client.request(method, BASE_URL + path, headers=self.headers, json=json, params=params)
            if res.status_code == 429:
                await asyncio.sleep(float(res.headers.get("Retry-After", 10)))
                continue
            if res.status_code >= 400:
                raise HubSpotError(f"{method} {path} -> HTTP {res.status_code}: {res.text[:300]}")
            return res.json() if res.content else {}
        raise HubSpotError(f"{method} {path} -> still rate limited after retries")

    async def list_all(self, object_type: str, properties: list) -> list:
        results, after = [], None
        while True:
            params = {"limit": 100, "properties": ",".join(properties), **({"after": after} if after else {})}
            data = await self.request("GET", f"/crm/v3/objects/{object_type}", params=params)
            results += data.get("results", [])
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                return results


def normalise_company(name: str) -> str:
    return COMPANY_SUFFIX_RE.sub("", re.sub(r"[^A-Z0-9]", "", str(name).upper()))


def _normalise_address(address: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(address).upper())


def _clean(value) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in ("nan", "unidentified") or "none found" in text.lower() else text


def _email(row) -> str:
    email = _clean(row.get("Employee Email")).lower()
    return email if "@" in email else ""


def _phone(row) -> str:
    return _clean(row.get("Phone Number"))


def _domain(row) -> str:
    domain = _clean(row.get("Discovered Domain")).lower()
    return domain.removeprefix("www.") if "." in domain else ""


# ---------------------------------------------------------------- setup


async def _find_project_object(hs: _Client) -> str:
    if HUBSPOT_PROJECT_OBJECT:
        return HUBSPOT_PROJECT_OBJECT
    schemas = (await hs.request("GET", "/crm/v3/schemas")).get("results", [])
    for schema in schemas:
        labels = schema.get("labels", {})
        if {str(labels.get("singular", "")).lower(), str(labels.get("plural", "")).lower()} & {PROJECT_LABEL, PROJECT_LABEL + "s"}:
            return schema["objectTypeId"]
    raise HubSpotError("Couldn't find a Project object in HubSpot - set HUBSPOT_PROJECT_OBJECT to its object type ID")


async def _property_map(hs: _Client, object_type: str, labels: dict) -> dict:
    """{export column: (internal name, type)} for the wanted labels."""
    found = {p["label"].strip().lower(): (p["name"], p.get("type", "string"))
             for p in (await hs.request("GET", f"/crm/v3/properties/{object_type}")).get("results", [])}
    mapping, missing = {}, []
    for column, label in labels.items():
        if label.lower() in found:
            mapping[column] = found[label.lower()]
        else:
            missing.append(label)
    if missing:
        logger.warning(f"HubSpot {object_type}: no property labelled {missing} - those columns won't be sent")
    return mapping


async def _pipeline_stage(hs: _Client, object_type: str) -> tuple:
    for pipeline in (await hs.request("GET", f"/crm/v3/pipelines/{object_type}")).get("results", []):
        if pipeline["label"].strip().lower() == PIPELINE_LABEL.lower():
            for stage in pipeline.get("stages", []):
                if stage["label"].strip().lower() == STAGE_LABEL.lower():
                    return pipeline["id"], stage["id"]
    raise HubSpotError(f"Couldn't find pipeline '{PIPELINE_LABEL}' with stage '{STAGE_LABEL}' on {object_type}")


def _properties(row: dict, mapping: dict) -> dict:
    """Maps export columns to HubSpot properties, converting dates and
    numbers and leaving out blanks (so nothing gets cleared)."""
    out = {}
    for column, (name, prop_type) in mapping.items():
        value = _clean(row.get(column))
        if column == "Employee Email":
            value = _email(row)
        elif column == "Discovered Domain":
            value = _domain(row)
        if not value:
            continue
        if prop_type in ("date", "datetime"):
            m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", value)
            if not m:
                continue
            value = f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
        elif prop_type == "number":
            value = re.sub(r"[^\d.]", "", value)
            if not value:
                continue
        out[name] = value
    return out


# ---------------------------------------------------------------- planning


def plan_push(results: list, existing_companies: list, existing_emails: set, existing_addresses: set) -> dict:
    """Works out what to create, without touching HubSpot. Returns new
    companies, contacts and projects (each linked to a company key that's
    either an existing HubSpot ID or a new company), plus skip counts."""
    by_name = {}
    for company in existing_companies:
        key = normalise_company(company["properties"].get("name") or "")
        if key:
            by_name.setdefault(key, []).append(company)
    used_domains = {(c["properties"].get("domain") or "").lower() for c in existing_companies} - {""}

    plan = {"companies": {}, "contacts": [], "projects": [], "skipped": {
        "contacts without email or phone": 0, "companies without a usable contact": 0,
        "contacts already in HubSpot": 0, "projects already in HubSpot": 0}}
    rows_by_company = {}
    for row in results:
        key = normalise_company(row.get("Target Company Name", ""))
        if key:
            rows_by_company.setdefault(key, []).append(row)

    seen_emails, seen_people, seen_projects = set(), set(), set()
    for key, rows in rows_by_company.items():
        usable = [r for r in rows if _email(r) or _phone(r)]
        plan["skipped"]["contacts without email or phone"] += len({_clean(r.get("Employee Name")) for r in rows if r not in usable})
        if not usable:
            plan["skipped"]["companies without a usable contact"] += 1
            continue

        if key in by_name:
            # Reuse the existing company untouched - prefer one with this domain.
            domains = {_domain(r) for r in rows}
            matches = by_name[key]
            company_ref = ("existing", next((c for c in matches if (c["properties"].get("domain") or "").lower() in domains), matches[0])["id"])
        else:
            first = rows[0]
            new_company = {"row": first, "domain": ""}
            domain = _domain(first)
            # A domain already on a differently named company would make
            # future imports match the wrong record, so it's left off.
            if domain and domain not in used_domains:
                new_company["domain"] = domain
                used_domains.add(domain)
            plan["companies"][key] = new_company
            company_ref = ("new", key)

        for row in usable:
            email = _email(row)
            person = email or (key, _clean(row.get("Employee Name")).upper(), _phone(row))
            if person in seen_people:
                continue
            seen_people.add(person)
            if email and (email in existing_emails or email in seen_emails):
                plan["skipped"]["contacts already in HubSpot"] += email in existing_emails
                continue
            seen_emails.add(email)
            plan["contacts"].append({"row": row, "company": company_ref})

        for row in rows:
            address = _normalise_address(row.get("Property Address", ""))
            if not address or (key, address) in seen_projects:
                continue
            seen_projects.add((key, address))
            if address in existing_addresses:
                plan["skipped"]["projects already in HubSpot"] += 1
                continue
            plan["projects"].append({"row": row, "company": company_ref})
    return plan


# ---------------------------------------------------------------- push


async def _batch_create(hs: _Client, object_type: str, items: list) -> list:
    """Creates records in batches; returns their IDs in input order (None
    for any that can't be matched back with certainty)."""
    ids = []
    for i in range(0, len(items), BATCH_SIZE):
        chunk = items[i:i + BATCH_SIZE]
        data = await hs.request("POST", f"/crm/v3/objects/{object_type}/batch/create", json={
            "inputs": [{"properties": p, "objectWriteTraceId": str(n)} for n, p in enumerate(chunk)]})
        # Batch results aren't guaranteed to come back in input order, so
        # each one is matched to its input by the trace ID echoed back. A
        # record that can't be matched gets no association rather than a
        # possibly wrong one.
        by_trace = {str(r.get("objectWriteTraceId")): r["id"] for r in data.get("results", []) if r.get("objectWriteTraceId") is not None}
        if not by_trace and len(data.get("results", [])) == len(chunk) == 1:
            by_trace = {"0": data["results"][0]["id"]}
        missing = len(chunk) - len(by_trace)
        if missing:
            logger.warning(f"HubSpot {object_type}: {missing} created record(s) couldn't be matched back - they won't be linked")
        ids += [by_trace.get(str(n)) for n in range(len(chunk))]
    return ids


async def _associate(hs: _Client, from_type: str, to_type: str, pairs: list):
    pairs = [(a, b) for a, b in pairs if a and b]
    for i in range(0, len(pairs), BATCH_SIZE):
        chunk = pairs[i:i + BATCH_SIZE]
        await hs.request("POST", f"/crm/v4/associations/{from_type}/{to_type}/batch/associate/default",
                         json={"inputs": [{"from": {"id": a}, "to": {"id": b}} for a, b in chunk]})


async def _existing_emails(hs: _Client, emails: list) -> set:
    found = set()
    for i in range(0, len(emails), BATCH_SIZE):
        chunk = emails[i:i + BATCH_SIZE]
        data = await hs.request("POST", "/crm/v3/objects/contacts/batch/read",
                                json={"idProperty": "email", "properties": ["email"], "inputs": [{"id": e} for e in chunk]})
        found |= {(r.get("properties", {}).get("email") or "").lower() for r in data.get("results", [])}
    return found


async def _phone_exists(hs: _Client, phone: str) -> bool:
    groups = [{"filters": [{"propertyName": prop, "operator": "EQ", "value": phone}]} for prop in ("phone", "mobilephone")]
    data = await hs.request("POST", "/crm/v3/objects/contacts/search", json={"filterGroups": groups, "limit": 1}, kind="search")
    return data.get("total", 0) > 0


async def push_results_to_hubspot(results: list, dry_run: bool = False, token: str = None,
                                  client: httpx.AsyncClient = None) -> dict:
    """Creates the new companies, contacts and projects from a job's
    results. With dry_run, works out and returns the same summary without
    creating anything."""
    token = token or HUBSPOT_ACCESS_TOKEN
    if not token:
        raise ValueError("HUBSPOT_ACCESS_TOKEN is not configured.")
    own_client = client is None
    client = client or httpx.AsyncClient(timeout=60.0)
    try:
        hs = _Client(client, token)
        project_type = await _find_project_object(hs)
        maps = {"companies": await _property_map(hs, "companies", PROPERTY_LABELS["companies"]),
                "contacts": await _property_map(hs, "contacts", PROPERTY_LABELS["contacts"]),
                "projects": await _property_map(hs, project_type, PROPERTY_LABELS["projects"])}
        address_prop = maps["projects"].get("Property Address", (None,))[0]
        if not address_prop:
            raise HubSpotError("The Project object has no 'Property Address' property")
        pipeline_id, stage_id = await _pipeline_stage(hs, project_type)

        existing_companies = await hs.list_all("companies", ["name", "domain"])
        existing_addresses = {_normalise_address(p.get("properties", {}).get(address_prop) or "")
                              for p in await hs.list_all(project_type, [address_prop])} - {""}
        emails = sorted({_email(r) for r in results} - {""})
        existing_emails = await _existing_emails(hs, emails)

        plan = plan_push(results, existing_companies, existing_emails, existing_addresses)

        # Phone-only contacts can't be matched by email, so check their phone.
        kept = []
        for contact in plan["contacts"]:
            row = contact["row"]
            if not _email(row) and await _phone_exists(hs, _phone(row)):
                plan["skipped"]["contacts already in HubSpot"] += 1
                continue
            kept.append(contact)
        plan["contacts"] = kept

        summary = {"dry_run": dry_run, "companies created": len(plan["companies"]),
                   "contacts created": len(plan["contacts"]), "projects created": len(plan["projects"]),
                   "existing companies linked": len({c["company"][1] for c in plan["contacts"] + plan["projects"] if c["company"][0] == "existing"}),
                   **plan["skipped"]}
        if dry_run:
            return summary

        company_keys = list(plan["companies"])
        company_props = []
        for key in company_keys:
            props = _properties(plan["companies"][key]["row"], maps["companies"])
            domain_prop = maps["companies"].get("Discovered Domain", (None,))[0]
            if domain_prop:
                props.pop(domain_prop, None)
                if plan["companies"][key]["domain"]:
                    props[domain_prop] = plan["companies"][key]["domain"]
            company_props.append(props)
        new_ids = dict(zip(company_keys, await _batch_create(hs, "companies", company_props)))
        company_id = lambda ref: ref[1] if ref[0] == "existing" else new_ids.get(ref[1])

        contact_ids = await _batch_create(hs, "contacts", [_properties(c["row"], maps["contacts"]) for c in plan["contacts"]])
        await _associate(hs, "contacts", "companies", [(cid, company_id(c["company"])) for cid, c in zip(contact_ids, plan["contacts"])])

        project_props = []
        for p in plan["projects"]:
            props = _properties(p["row"], maps["projects"])
            props.update({"hs_pipeline": pipeline_id, "hs_pipeline_stage": stage_id})
            project_props.append(props)
        project_ids = await _batch_create(hs, project_type, project_props)
        await _associate(hs, project_type, "companies", [(pid, company_id(p["company"])) for pid, p in zip(project_ids, plan["projects"])])

        logger.info(f"HubSpot push complete: {summary}")
        return summary
    finally:
        if own_client:
            await client.aclose()
