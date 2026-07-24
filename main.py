import os
import io
import time
import logging
import asyncio
import uuid
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional, Any, Set
from pydantic import BaseModel

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
from urllib.parse import quote_plus, urlparse
from jinja2 import Template

# --- 0. Set Up Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger("ECA_Pipeline")

# --- 1. Define Core Objects and Data Structures ---

class DomainInfo(BaseModel):
    provided_name: str
    discovered_domain: Optional[str] = None
    discovery_method: Optional[str] = None
    search_query_used: Optional[str] = None

class EmployeeEmail(BaseModel):
    email: str
    type: str
    verification: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    position: Optional[str] = None
    source: Optional[str] = None

class ContactDiscoveryResult(BaseModel):
    domain: str
    emails_found: List[EmployeeEmail] = []
    hunter_score: Optional[float] = None
    organization_name: Optional[str] = None

class CHCompanyData(BaseModel):
    company_name: str
    company_number: Optional[str] = None
    status: Optional[str] = None
    registered_address: Optional[str] = None
    directors: List[Dict[str, str]] = []

class PipelineJob(BaseModel):
    id: str
    status: str
    created_at: float
    updated_at: float
    used_serper: bool = False
    used_companies_house: bool = False
    used_hunter: bool = False
    used_fullenrich: bool = False
    used_email_outreach: bool = False

    # Progress trackers
    total_companies_in: int = 0
    domains_discovered_count: int = 0
    domains_with_emails_count: int = 0
    total_emails_found_count: int = 0
    ch_companies_found_count: int = 0
    outreach_emails_sent_count: int = 0

    discovered_domains: List[Dict[str, str]] = []
    discovered_contacts: List[Dict[str, Any]] = []
    ch_data: List[Dict[str, Any]] = []
    final_combined_results: List[Dict[str, Any]] = []

# --- 2. Initialize FastAPI Application ---

app = FastAPI(title="ECA Cold Email Pipeline", description="Lead Enrichment & Automated Outreach API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 3. Persistent Data Mock & Blacklist ---
pipeline_jobs: Dict[str, PipelineJob] = {}

BLACKLIST_FILE = "blacklist.txt"

def load_blacklist() -> set:
    if not os.path.exists(BLACKLIST_FILE):
        return set()
    with open(BLACKLIST_FILE, "r") as f:
        return set(line.strip().lower() for line in f if line.strip())

def add_to_blacklist(email: str):
    with open(BLACKLIST_FILE, "a") as f:
        f.write(f"{email.lower()}\n")

# --- 4. Define API Endpoints ---

@app.get("/")
async def root():
    return {"message": "ECA Pipeline API is operational on Render."}

@app.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe(email: str):
    if email:
        add_to_blacklist(email)
        logger.info(f"Unsubscribed: {email}")
        return f"""
        <html>
            <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                <h2>Unsubscribed Successfully</h2>
                <p><b>{email}</b> has been securely removed from our mailing list.</p>
            </body>
        </html>
        """
    return "Invalid request."

@app.post("/upload-leads/", response_model=PipelineJob)
async def upload_leads_csv_start_pipeline(
    file: UploadFile = File(...),
    serper_api_key: Optional[str] = Form(None),
    ch_api_key: Optional[str] = Form(None),
    hunter_api_key: Optional[str] = Form(None),
    fullenrich_api_key: Optional[str] = Form(None),
    send_emails: bool = Form(False),
    smtp_user: Optional[str] = Form(None),
    smtp_pass: Optional[str] = Form(None),
    max_contacts_per_domain: int = Form(10),
    hunter_email_verification: bool = Form(True),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    if not file.filename.lower().endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    file_bytes_captured = await file.read()
    original_filename = file.filename

    job_id = f"eca_job_{uuid.uuid4().hex[:8]}"
    current_time = time.time()

    new_job = PipelineJob(
        id=job_id,
        status="Processing",
        created_at=current_time,
        updated_at=current_time,
        used_serper=True if serper_api_key else False,
        used_companies_house=True if ch_api_key else False,
        used_hunter=True if hunter_api_key else False,
        used_fullenrich=True if fullenrich_api_key else False,
        used_email_outreach=send_emails
    )
    pipeline_jobs[job_id] = new_job
    logger.info(f"Initialized job {job_id}. Status: {new_job.status}")

    background_tasks.add_task(
        execute_full_pipeline,
        job_id,
        file_bytes_captured,
        original_filename,
        serper_api_key,
        ch_api_key,
        hunter_api_key,
        fullenrich_api_key,
        send_emails,
        smtp_user,
        smtp_pass,
        max_contacts_per_domain,
        hunter_email_verification
    )

    return new_job

@app.get("/pipeline-status/{job_id}", response_model=PipelineJob)
async def get_pipeline_job_status(job_id: str):
    job_record = pipeline_jobs.get(job_id)
    if not job_record:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
    return job_record

@app.get("/pipeline-results-csv/{job_id}")
async def download_combined_results_csv(job_id: str):
    return {"message": "Placeholder for generated CSV output.", "job_id": job_id}

# --- 5. Full Asynchronous Pipeline Logic (Background Task) ---

async def execute_full_pipeline(
    job_id: str,
    file_bytes: bytes,
    filename_for_type: str,
    serper_api_key: Optional[str],
    ch_api_key: Optional[str],
    hunter_api_key: Optional[str],
    fullenrich_api_key: Optional[str],
    send_emails: bool,
    smtp_user: Optional[str],
    smtp_pass: Optional[str],
    max_contacts_per_domain: int,
    hunter_email_verification: bool
):
    logger.info(f"Background Pipeline Starting for Job: {job_id}")
    job = pipeline_jobs[job_id]

    try:
        target_company_names = await preprocess_uploaded_file(job, file_bytes, filename_for_type)

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Step B: Serper Domain Discovery
            if job.used_serper and serper_api_key:
                domain_discovery_results = await run_serper_domain_discovery(job, client, target_company_names, serper_api_key)
            else:
                domain_discovery_results = []

            # Step C: Companies House Verification & Fallback Data
            if job.used_companies_house and ch_api_key:
                ch_results = await run_companies_house_enrichment(job, client, target_company_names, ch_api_key)
            else:
                ch_results = []

            # Step D: Hunter.io Contact Discovery
            if job.used_hunter and hunter_api_key and domain_discovery_results:
                discovered_contacts = await run_hunter_contact_discovery(job, client, domain_discovery_results, hunter_api_key, max_contacts_per_domain)
            else:
                discovered_contacts = []

            # Step E: FullEnrich Placeholder
            enrichment_results = []

            # Step F: Final Data Combination
            await combine_and_finalize_results(job, target_company_names, domain_discovery_results, discovered_contacts, ch_results)

            # Step G: Email Outreach execution
            if send_emails and smtp_user and smtp_pass:
                await send_outreach_emails(job, smtp_user, smtp_pass)

    except Exception as e:
        logger.error(f"Critical error in pipeline job {job_id}: {str(e)}")
        job.status = "Error"

    finally:
        if job.status != "Error":
            job.status = "Complete"
        job.updated_at = time.time()
        logger.info(f"Background Pipeline Finished for Job: {job_id}. Status: {job.status}")


# --- 6. Pipeline Step Worker Functions ---

async def preprocess_uploaded_file(job: PipelineJob, file_contents: bytes, filename: str) -> List[str]:
    logger.info(f"Job {job.id}: Starting preprocessing.")
    try:
        if filename.lower().endswith('.csv'):
            try:
                content_stream = io.StringIO(file_contents.decode('utf-8'))
                df = pd.read_csv(content_stream)
            except UnicodeDecodeError:
                content_stream = io.StringIO(file_contents.decode('iso-8859-1'))
                df = pd.read_csv(content_stream)
        else:
            df = pd.read_excel(io.BytesIO(file_contents))

        potential_company_cols = ['Proprietor Name (1)', 'Recipient Name', 'company_name', 'company', 'name']
        columns_to_check_lower = [c.lower() for c in potential_company_cols]
        found_col = next((col for col in df.columns if col.lower() in columns_to_check_lower), None)

        if not found_col:
             raise HTTPException(status_code=400, detail="Required company name column not found.")

        raw_names = df[found_col].dropna().unique()
        clean_company_names = [str(name).strip() for name in raw_names if str(name).strip()]

        job.total_companies_in = len(clean_company_names)
        return clean_company_names

    except Exception as e:
        logger.error(f"Preprocessing error: {e}")
        raise

async def run_serper_domain_discovery(job: PipelineJob, client: httpx.AsyncClient, company_names: List[str], api_key: str) -> List[DomainInfo]:
    results = []
    discovered_domains_set: Set[str] = set()

    for name in company_names:
        search_name = re.sub(r'[^a-zA-Z0-9\s]', '', name).strip()
        query = f"{search_name} UK website"
        serper_url = "https://google.serper.dev/search"
        headers = {'X-API-KEY': api_key, 'Content-Type': 'application/json'}
        payload = {"q": query, "gl": "gb"}
        discovered_domain = None

        try:
            response = await client.post(serper_url, headers=headers, json=payload)
            if response.status_code == 200:
                organic_results = response.json().get('organic', [])
                for item in organic_results:
                    item_link = item.get('link')
                    if not item_link: continue
                    netloc = urlparse(item_link).netloc.lower()

                    massive_dirs = ['.gov.uk', '.linkedin.com', '.facebook.com', '.instagram.com',
                                    'companieshouse.gov.uk', 'endole.co.uk', 'yell.com', '192.com',
                                    'dnb.com', 'bloomberg.com', 'zoominfo.com', 'check-a-trade.com']
                    if any(dir_root in netloc for dir_root in massive_dirs):
                        continue

                    raw_domain = netloc.replace('www.', '')
                    domain_clean = re.sub(r'[^a-z0-9]', '', raw_domain.split('.')[0])

                    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', name.replace('&', ' and ')).lower()
                    stop_words = {'limited', 'ltd', 'uk', 'plc', 'group', 'co', 'corp', 'the', 'inc', 'and'}
                    tokens = [w for w in clean_name.split() if w not in stop_words]

                    if tokens:
                        matched_tokens = [t for t in tokens if t in domain_clean]
                        if not matched_tokens:
                            continue

                        if not any(len(t) >= 4 for t in matched_tokens):
                            matched_chars = sum(len(t) for t in matched_tokens)
                            for sw in ['ltd', 'group', 'uk', 'co']:
                                if domain_clean.endswith(sw): matched_chars += len(sw)

                            if (len(domain_clean) > 0) and (matched_chars / len(domain_clean)) < 0.4:
                                logger.info(f"Rejected domain '{raw_domain}' for '{name}': Short tokens below 40% threshold.")
                                continue

                    discovered_domain = raw_domain
                    break
        except httpx.RequestError as e:
            logger.error(f"Serper error: {e}")

        if discovered_domain and discovered_domain not in discovered_domains_set:
            results.append(DomainInfo(provided_name=name, discovered_domain=discovered_domain, discovery_method='SerperSearch'))
            discovered_domains_set.add(discovered_domain)
        else:
            results.append(DomainInfo(provided_name=name, discovered_domain=None))

    job.domains_discovered_count = len(discovered_domains_set)
    return results

# --- Step C: Real Companies House API Integration ---
async def run_companies_house_enrichment(job: PipelineJob, client: httpx.AsyncClient, company_names: List[str], api_key: str) -> List[CHCompanyData]:
    logger.info(f"Job {job.id}: Querying Companies House API...")
    results = []

    # Companies House uses HTTP Basic Auth with API key as username and empty password
    auth = (api_key, "")

    for name in company_names:
        ch_info = CHCompanyData(company_name=name)
        try:
            # 1. Search for company by name
            search_url = f"https://api.company-information.service.gov.uk/search/companies?q={quote_plus(name)}"
            res = await client.get(search_url, auth=auth)

            if res.status_code == 200:
                items = res.json().get("items", [])
                if items:
                    top_match = items[0]
                    comp_num = top_match.get("company_number")
                    ch_info.company_number = comp_num
                    ch_info.status = top_match.get("company_status")

                    # Construct registered address
                    addr_obj = top_match.get("address", {})
                    addr_parts = [addr_obj.get(k) for k in ["address_line_1", "locality", "postal_code"] if addr_obj.get(k)]
                    ch_info.registered_address = ", ".join(addr_parts) if addr_parts else top_match.get("address_snippet")

                    # 2. Fetch active officers / directors
                    if comp_num:
                        officers_url = f"https://api.company-information.service.gov.uk/company/{comp_num}/officers"
                        off_res = await client.get(officers_url, auth=auth)
                        if off_res.status_code == 200:
                            officer_items = off_res.json().get("items", [])
                            directors = []
                            for off in officer_items:
                                # Keep active directors
                                if off.get("officer_role") in ["director", "nominee-director"] and not off.get("resigned_on"):
                                    directors.append({
                                        "name": off.get("name"),
                                        "role": off.get("officer_role", "Director").title(),
                                        "occupation": off.get("occupation", "Director")
                                    })
                            ch_info.directors = directors

        except Exception as e:
            logger.error(f"Companies House API error for {name}: {e}")

        results.append(ch_info)

    job.ch_companies_found_count = len([r for r in results if r.company_number])
    logger.info(f"Job {job.id}: Companies House lookup complete. Matched {job.ch_companies_found_count} records.")
    return results

async def run_hunter_contact_discovery(job: PipelineJob, client: httpx.AsyncClient, discovery_results: List[DomainInfo], api_key: str, max_per_domain: int) -> List[ContactDiscoveryResult]:
    results = []
    total_emails = 0
    domains_to_process = [r.discovered_domain for r in discovery_results if r.discovered_domain]

    # Prefix blocklist to filter role-based inboxes
    generic_prefixes = ('info@', 'admin@', 'customercare@', 'customer.service@', 'onlinehelp@', 'sales@', 'support@', 'help@', 'contact@', 'office@', 'enquiries@')

    for domain in domains_to_process:
        hunter_url = f"https://api.hunter.io/v2/domain-search?domain={quote_plus(domain)}&limit={max_per_domain}&api_key={api_key}"
        discovery_result = ContactDiscoveryResult(domain=domain)

        try:
            response = await client.get(hunter_url)
            if response.status_code == 200:
                hunter_data = response.json().get('data', {})
                emails_found = hunter_data.get('emails', [])
                if emails_found:
                    structured_emails = []
                    for e in emails_found:
                        email_val = e.get('value', '').lower()

                        # Filter explicit generic prefix inboxes, but KEEP legitimate named emails
                        if any(email_val.startswith(p) for p in generic_prefixes):
                            continue

                        employee = EmployeeEmail(
                            email=email_val,
                            type=e.get('type', 'personal'),
                            first_name=e.get('first_name'),
                            last_name=e.get('last_name'),
                            position=e.get('position')
                        )
                        structured_emails.append(employee)

                    discovery_result.emails_found = structured_emails
                    total_emails += len(structured_emails)
        except httpx.RequestError as e:
            logger.error(f"Hunter error: {e}")

        results.append(discovery_result)

    job.total_emails_found_count = total_emails
    return results

async def combine_and_finalize_results(
    job: PipelineJob,
    company_names: List[str],
    domains: List[DomainInfo],
    contacts: List[ContactDiscoveryResult],
    ch_data: List[CHCompanyData]
):
    combined_results = []
    domain_map = {r.provided_name: (r.discovered_domain if r.discovered_domain else None) for r in domains}
    contact_map = {r.domain: r.emails_found for r in contacts}
    ch_map = {c.company_name: c for c in ch_data}

    for company_name in company_names:
        discovered_domain = domain_map.get(company_name)
        emails = contact_map.get(discovered_domain) if discovered_domain else None
        ch_info = ch_map.get(company_name)

        # Base CH fallback details
        ch_number = ch_info.company_number if ch_info else "NOT FOUND"
        ch_address = ch_info.registered_address if ch_info else "NOT FOUND"
        primary_director = ch_info.directors[0]["name"] if (ch_info and ch_info.directors) else "UNIDENTIFIED"

        # Case 1: Web & Contact Discovery Success
        if discovered_domain and emails:
            for email in emails:
                person_name = f"{email.first_name} {email.last_name}" if (email.first_name and email.last_name) else primary_director
                combined_results.append({
                    "Target Company Name": company_name,
                    "Discovered Domain": discovered_domain,
                    "Employee Name": person_name,
                    "Employee Position": email.position if email.position else "Director",
                    "Employee Email": email.email,
                    "Companies House Number": ch_number,
                    "Registered Address": ch_address,
                    "Discovery Pipeline Status": "Complete (Email Found)"
                })
            continue

        # Case 2: Domain Found, but Hunter found no emails
        if discovered_domain and not emails:
            combined_results.append({
                "Target Company Name": company_name,
                "Discovered Domain": discovered_domain,
                "Employee Name": primary_director,
                "Employee Position": "Director (Companies House)",
                "Employee Email": "NONE FOUND (Hunter)",
                "Companies House Number": ch_number,
                "Registered Address": ch_address,
                "Discovery Pipeline Status": "Domain Found (No Email)"
            })
            continue

        # Case 3: Domain Failed (SPV / No Website) -> Companies House Fallback
        combined_results.append({
            "Target Company Name": company_name,
            "Discovered Domain": "NONE FOUND (Serper)",
            "Employee Name": primary_director,
            "Employee Position": "Director (Companies House)",
            "Employee Email": "NONE FOUND (Serper/SPV)",
            "Companies House Number": ch_number,
            "Registered Address": ch_address,
            "Discovery Pipeline Status": "SPV Fallback (Companies House)"
        })

    job.final_combined_results = combined_results

async def send_outreach_emails(job: PipelineJob, smtp_user: str, smtp_pass: str):
    logger.info(f"Job {job.id}: Starting email outreach...")
    blacklist = load_blacklist()

    base_url = os.environ.get("RENDER_EXTERNAL_URL", "http://127.0.0.1:8000")

    template_str = """
    <p>Hi {{ name }},</p>
    <p>We noticed {{ company }} recently made a property purchase. We think you might be eligible for significant capital allowances.</p>
    <p>Best,<br>ECA Tax Associates</p>
    <br>
    <p style="font-size: 10px; color: gray;">
        To stop receiving these emails, <a href="{{ base_url }}/unsubscribe?email={{ email }}">click here to unsubscribe</a>.
    </p>
    """
    jinja_template = Template(template_str)

    try:
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(smtp_user, smtp_pass)
    except Exception as e:
        logger.error(f"Failed to connect to SMTP: {e}")
        job.status = "Error (SMTP Auth Failed)"
        return

    emails_sent = 0
    for row in job.final_combined_results:
        target_email = row.get("Employee Email", "").lower()

        if "none found" in target_email or target_email in ["unidentified", ""]:
            continue
        if target_email in blacklist:
            logger.info(f"Skipping {target_email} - On Blacklist.")
            continue

        first_name = row.get("Employee Name", "there").split()[0]
        if first_name == "UNIDENTIFIED": first_name = "there"

        html_body = jinja_template.render(
            name=first_name,
            company=row.get("Target Company Name", "your company"),
            email=target_email,
            base_url=base_url
        )

        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = target_email
        msg['Subject'] = f"Capital Allowances for {row.get('Target Company Name')}"
        msg.attach(MIMEText(html_body, 'html'))

        try:
            server.send_message(msg)
            emails_sent += 1
            logger.info(f"Sent email to {target_email}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Failed to send to {target_email}: {e}")

    server.quit()
    job.outreach_emails_sent_count = emails_sent
    logger.info(f"Job {job.id}: Email outreach complete. Sent {emails_sent} emails.")


# --- 7. Main Execution Entry Point ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info")
