"""Tool 1: the lead enrichment pipeline.

Reads an uploaded sheet, discovers each company's domain (Serper), pulls its
Companies House record, finds named employee emails at the domain (Hunter),
and combines it all into one row-per-contact result set.
"""
import asyncio
import io
import re
import time

import httpx
import pandas as pd
from fastapi import HTTPException

import companies_house
import hunter
import serper
import store
from hunter import split_ch_name
from config import MAX_COMPANIES_PER_UPLOAD, logger

PROPRIETOR_NAME_COL_RE = re.compile(r'^Proprietor Name \(\d+\)$', re.IGNORECASE)
GENERIC_NAME_COLS = ('Recipient Name', 'company_name', 'company', 'name')


async def execute_full_pipeline(job_id, file_bytes, filename, serper_api_key, ch_api_key, hunter_api_key, max_directors):
    job = store.pipeline_jobs[job_id]
    try:
        company_names, company_metadata = preprocess_uploaded_file(job, file_bytes, filename)

        if len(company_names) > MAX_COMPANIES_PER_UPLOAD:
            raise ValueError(
                f"Upload contains {len(company_names)} companies, which exceeds the "
                f"{MAX_COMPANIES_PER_UPLOAD} limit per run. Split the file and re-upload."
            )

        async with httpx.AsyncClient(timeout=30.0) as client:
            # company_metadata carries known Companies House numbers (from
            # the source sheet), used for exact lookups instead of a fuzzy
            # name search that can match the wrong company.
            ch_data = await companies_house.enrich_companies(job, client, company_names, company_metadata, ch_api_key) if ch_api_key else []
            ch_map = {c.company_name: c for c in ch_data}

            # CH's registered locality (town/city) is used to geo-target the
            # domain search and disambiguate same-named companies in
            # different countries (see serper.py's module docstring).
            locality_map = {name: c.registered_locality for name, c in ch_map.items() if c.registered_locality}
            domains = await serper.discover_domains(job, client, company_names, serper_api_key, locality_map) if serper_api_key else []
            dom_map = {d.provided_name: d.discovered_domain for d in domains}

            # Hunter is targeted at the specific directors Companies House
            # returned, so it needs both maps rather than just the domain list.
            contacts_by_company = (
                await hunter.discover_director_emails(job, client, company_names, dom_map, ch_map, hunter_api_key, max_directors)
                if hunter_api_key else {}
            )
            job.final_combined_results = combine_results(company_names, domains, contacts_by_company, ch_data, company_metadata)
    except Exception as e:
        logger.error(f"Pipeline error {job_id}: {e}")
        job.status = "Error"
    finally:
        if job.status != "Error":
            job.status = "Complete"
        job.updated_at = time.time()
        # Copied off-box so the result outlives a Render spin-down - see
        # store.py. Errors are persisted too, so the UI shows "Failed"
        # rather than "Expired" after a restart.
        await asyncio.to_thread(store.persist_pipeline_job, job)


def preprocess_uploaded_file(job, file_contents: bytes, filename: str):
    """Parses the upload and returns (company_names, company_metadata).

    company_metadata maps each company name -> {Property Address, Purchase
    Year, company_number}. Land Registry-style exports list up to four
    proprietors per title, each with its own registration number, so those
    are all extracted as separate leads rather than just the first one.
    """
    df = _read_sheet(file_contents, filename)
    proprietor_cols = [c for c in df.columns if PROPRIETOR_NAME_COL_RE.match(c.strip())]
    generic_col = next((c for c in df.columns if c.lower() in [x.lower() for x in GENERIC_NAME_COLS]), None)

    if not proprietor_cols and not generic_col:
        raise HTTPException(status_code=400, detail="Company column not found.")

    df['Purchase Year'] = _derive_purchase_year(df)

    company_metadata = _extract_companies(df, proprietor_cols, generic_col)
    names = list(company_metadata.keys())
    job.total_companies_in = len(names)
    return names, company_metadata


def _read_sheet(file_contents: bytes, filename: str) -> pd.DataFrame:
    if filename.lower().endswith('.csv'):
        try:
            return pd.read_csv(io.StringIO(file_contents.decode('utf-8')))
        except UnicodeDecodeError:
            return pd.read_csv(io.StringIO(file_contents.decode('iso-8859-1')))
    return pd.read_excel(io.BytesIO(file_contents))


def _derive_purchase_year(df: pd.DataFrame) -> pd.Series:
    if 'Date Proprietor Added' not in df.columns:
        return pd.Series('Unknown', index=df.index)

    raw = df['Date Proprietor Added']

    # Land Registry CSV exports store this as a UK-format text date
    # (e.g. "9/9/2024" or "28-09-2018", day-first, mixed separators) -
    # format='mixed' is required or pandas' batch parser bails out on the
    # whole column as soon as it hits an inconsistent separator. Excel
    # exports of the same data can instead serialize it as a numeric day
    # count, so anything that doesn't parse as a text date falls back to
    # that interpretation.
    parsed = pd.to_datetime(raw, dayfirst=True, format='mixed', errors='coerce')

    still_missing = parsed.isna()
    if still_missing.any():
        serial = pd.to_datetime(
            pd.to_numeric(raw[still_missing], errors='coerce'),
            unit='D', origin='1899-12-30', errors='coerce'
        )
        parsed.loc[still_missing] = serial

    years = parsed.dt.year
    # .astype('string') (nullable StringDtype) is required here - plain
    # .astype(str) on a nullable Int64 column stringifies missing values
    # as the literal text "NaN" instead of pd.NA, which silently defeats
    # a NaN-string replace() and is why "nan" was leaking into the output.
    return years.astype('Int64').astype('string').fillna('Unknown')


def _extract_companies(df: pd.DataFrame, proprietor_cols: list, generic_col: str) -> dict:
    company_metadata = {}   # normalized (upper) name -> metadata
    display_name = {}       # normalized name -> original-cased name

    def add(name, reg_no, prop_addr, purch_year, price_paid):
        name = str(name).strip()
        if not name or name.lower() == 'nan':
            return
        key = name.upper()
        if key in company_metadata:
            return  # already captured this company (case-insensitive)
        display_name[key] = name
        clean_reg_no = str(reg_no).strip() if reg_no not in (None, '') and str(reg_no).lower() != 'nan' else None
        company_metadata[key] = {
            'Property Address': prop_addr,
            'Purchase Year': purch_year,
            'Price Paid': price_paid,
            'company_number': clean_reg_no,
        }

    for _, row in df.iterrows():
        prop_addr = str(row.get('Property Address', 'your property'))
        purch_year = str(row.get('Purchase Year', 'recently'))
        price_paid = str(row.get('Price Paid', 'Unknown'))
        if proprietor_cols:
            for name_col in proprietor_cols:
                idx_match = re.search(r'\((\d+)\)$', name_col.strip())
                idx = idx_match.group(1) if idx_match else '1'
                reg_no_col = f'Company Registration No. ({idx})'
                reg_no = row.get(reg_no_col) if reg_no_col in df.columns else None
                add(row.get(name_col), reg_no, prop_addr, purch_year, price_paid)
        else:
            add(row.get(generic_col), None, prop_addr, purch_year, price_paid)

    return {display_name[key]: meta for key, meta in company_metadata.items()}


def combine_results(company_names, domains, contacts_by_company, ch_data, company_metadata) -> list:
    dom_map = {r.provided_name: r.discovered_domain for r in domains}
    conf_map = {r.provided_name: r.match_confidence for r in domains}
    ch_map = {c.company_name: c for c in ch_data}

    combined = []
    for name in company_names:
        combined.extend(_rows_for_company(name, dom_map, conf_map, contacts_by_company, ch_map, company_metadata))
    return combined


def _rows_for_company(name, dom_map, conf_map, contacts_by_company, ch_map, company_metadata) -> list:
    domain = dom_map.get(name)
    confidence = conf_map.get(name)
    contact_result = contacts_by_company.get(name)
    emails = contact_result.emails_found if contact_result else None
    ch = ch_map.get(name)
    meta = company_metadata.get(name, {})

    if ch and ch.directors:
        # CH officer names come as "SURNAME, First Middle" - split_ch_name
        # (shared with hunter.py's Hunter lookups) turns that into a normal
        # "First Last" display order instead of showing the raw CH format.
        director_first, director_last = split_ch_name(ch.directors[0]["name"])
        if not director_first:
            # Name didn't fit the expected pattern (e.g. a single word) -
            # fall back to showing it as-is rather than dropping it.
            director_first, director_last = ch.directors[0]["name"], ""
    else:
        director_first, director_last = "UNIDENTIFIED", ""
    director = f"{director_first} {director_last}".strip()

    base = {
        "Target Company Name": name,
        "Property Address": meta.get('Property Address', 'your property'),
        "Purchase Year": meta.get('Purchase Year', 'recently'),
        "Price Paid": meta.get('Price Paid', 'Unknown'),
        "Companies House Number": ch.company_number if ch else "NOT FOUND",
        "Registered Address": ch.registered_address if ch else "NOT FOUND",
    }

    if domain and emails:
        rows = []
        for e in emails:
            # Every email here was looked up by a specific director's name,
            # so e.first_name/last_name always correspond to that director -
            # no generic-inbox fallback needed.
            person = f"{e.first_name} {e.last_name}"
            rows.append({**base, "Discovered Domain": domain, "Domain Match Confidence": confidence,
                         "Employee Name": person, "Employee Forename": e.first_name, "Employee Surname": e.last_name,
                         "Employee Position": e.position or "Director",
                         "Employee Email": e.email, "Status": "Complete (Email Found)"})
        return rows
    if domain and ch and ch.directors:
        return [{**base, "Discovered Domain": domain, "Domain Match Confidence": confidence,
                  "Employee Name": director, "Employee Forename": director_first, "Employee Surname": director_last,
                  "Employee Position": "Director (CH)",
                  "Employee Email": "NONE FOUND (Hunter)", "Status": "Domain Found (No Director Email)"}]
    if domain:
        return [{**base, "Discovered Domain": domain, "Domain Match Confidence": confidence,
                  "Employee Name": director, "Employee Forename": director_first, "Employee Surname": director_last,
                  "Employee Position": "N/A",
                  "Employee Email": "NONE FOUND (No CH Directors)", "Status": "Domain Found (No CH Directors)"}]
    if ch and ch.directors:
        return [{**base, "Discovered Domain": "NONE FOUND (Serper)", "Domain Match Confidence": None,
                  "Employee Name": director, "Employee Forename": director_first, "Employee Surname": director_last,
                  "Employee Position": "Director (CH)",
                  "Employee Email": "NONE FOUND (SPV)", "Status": "SPV Fallback (CH)"}]
    return [{**base, "Discovered Domain": "NONE FOUND (Serper)", "Domain Match Confidence": None,
              "Employee Name": director, "Employee Forename": director_first, "Employee Surname": director_last,
              "Employee Position": "N/A",
              "Employee Email": "NONE FOUND", "Status": "Not Found"}]


def results_to_csv_bytes(results: list) -> bytes:
    """Serializes a job's final_combined_results into CSV bytes with the
    exact column headers /bulk-send/ expects, so the export can be fed
    straight back in without editing."""
    df = pd.DataFrame(results)
    return df.to_csv(index=False).encode('utf-8')
