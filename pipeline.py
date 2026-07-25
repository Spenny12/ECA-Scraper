"""Tool 1: the lead enrichment pipeline.

Reads an uploaded sheet, discovers each company's domain (Serper), pulls its
Companies House record, finds named employee emails at the domain (Hunter),
and combines it all into one row-per-contact result set.
"""
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
from config import MAX_COMPANIES_PER_UPLOAD, logger

PROPRIETOR_NAME_COL_RE = re.compile(r'^Proprietor Name \(\d+\)$', re.IGNORECASE)
GENERIC_NAME_COLS = ('Recipient Name', 'company_name', 'company', 'name')


async def execute_full_pipeline(job_id, file_bytes, filename, serper_api_key, ch_api_key, hunter_api_key, max_contacts):
    job = store.pipeline_jobs[job_id]
    try:
        company_names, company_metadata = preprocess_uploaded_file(job, file_bytes, filename)

        if len(company_names) > MAX_COMPANIES_PER_UPLOAD:
            raise ValueError(
                f"Upload contains {len(company_names)} companies, which exceeds the "
                f"{MAX_COMPANIES_PER_UPLOAD} limit per run. Split the file and re-upload."
            )

        async with httpx.AsyncClient(timeout=30.0) as client:
            domains = await serper.discover_domains(job, client, company_names, serper_api_key) if serper_api_key else []
            # company_metadata carries known Companies House numbers (from
            # the source sheet), used for exact lookups instead of a fuzzy
            # name search that can match the wrong company.
            ch_data = await companies_house.enrich_companies(job, client, company_names, company_metadata, ch_api_key) if ch_api_key else []
            contacts = await hunter.discover_contacts(job, client, domains, hunter_api_key, max_contacts) if hunter_api_key else []
            job.final_combined_results = combine_results(company_names, domains, contacts, ch_data, company_metadata)
    except Exception as e:
        logger.error(f"Pipeline error {job_id}: {e}")
        job.status = "Error"
    finally:
        if job.status != "Error":
            job.status = "Complete"
        job.updated_at = time.time()


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
    # Land Registry dates are Excel serial day numbers.
    years = pd.to_datetime(pd.to_numeric(df['Date Proprietor Added'], errors='coerce'), unit='D', origin='1899-12-30').dt.year
    return years.astype('Int64').astype(str).replace('<NA>', 'Unknown')


def _extract_companies(df: pd.DataFrame, proprietor_cols: list, generic_col: str) -> dict:
    company_metadata = {}   # normalized (upper) name -> metadata
    display_name = {}       # normalized name -> original-cased name

    def add(name, reg_no, prop_addr, purch_year):
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
            'company_number': clean_reg_no,
        }

    for _, row in df.iterrows():
        prop_addr = str(row.get('Property Address', 'your property'))
        purch_year = str(row.get('Purchase Year', 'recently'))
        if proprietor_cols:
            for name_col in proprietor_cols:
                idx_match = re.search(r'\((\d+)\)$', name_col.strip())
                idx = idx_match.group(1) if idx_match else '1'
                reg_no_col = f'Company Registration No. ({idx})'
                reg_no = row.get(reg_no_col) if reg_no_col in df.columns else None
                add(row.get(name_col), reg_no, prop_addr, purch_year)
        else:
            add(row.get(generic_col), None, prop_addr, purch_year)

    return {display_name[key]: meta for key, meta in company_metadata.items()}


def combine_results(company_names, domains, contacts, ch_data, company_metadata) -> list:
    dom_map = {r.provided_name: r.discovered_domain for r in domains}
    con_map = {r.domain: r.emails_found for r in contacts}
    ch_map = {c.company_name: c for c in ch_data}

    combined = []
    for name in company_names:
        combined.extend(_rows_for_company(name, dom_map, con_map, ch_map, company_metadata))
    return combined


def _rows_for_company(name, dom_map, con_map, ch_map, company_metadata) -> list:
    domain = dom_map.get(name)
    emails = con_map.get(domain) if domain else None
    ch = ch_map.get(name)
    meta = company_metadata.get(name, {})

    director = ch.directors[0]["name"] if (ch and ch.directors) else "UNIDENTIFIED"
    base = {
        "Target Company Name": name,
        "Property Address": meta.get('Property Address', 'your property'),
        "Purchase Year": meta.get('Purchase Year', 'recently'),
        "Companies House Number": ch.company_number if ch else "NOT FOUND",
        "Registered Address": ch.registered_address if ch else "NOT FOUND",
    }

    if domain and emails:
        rows = []
        for e in emails:
            person = f"{e.first_name} {e.last_name}" if e.first_name else director
            rows.append({**base, "Discovered Domain": domain, "Employee Name": person,
                         "Employee Position": e.position or "Director", "Employee Email": e.email,
                         "Status": "Complete (Email Found)"})
        return rows
    if domain:
        return [{**base, "Discovered Domain": domain, "Employee Name": director,
                  "Employee Position": "Director (CH)", "Employee Email": "NONE FOUND (Hunter)",
                  "Status": "Domain Found (No Email)"}]
    return [{**base, "Discovered Domain": "NONE FOUND (Serper)", "Employee Name": director,
              "Employee Position": "Director (CH)", "Employee Email": "NONE FOUND (SPV)",
              "Status": "SPV Fallback (CH)"}]


def results_to_csv_bytes(results: list) -> bytes:
    """Serializes a job's final_combined_results into CSV bytes with the
    exact column headers /bulk-send/ expects, so the export can be fed
    straight back in without editing."""
    df = pd.DataFrame(results)
    return df.to_csv(index=False).encode('utf-8')
