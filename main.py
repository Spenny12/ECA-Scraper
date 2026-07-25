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
from typing import List, Dict, Optional, Any, Set, Tuple
from pydantic import BaseModel

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
from urllib.parse import quote_plus, urlparse
from jinja2 import Template
from supabase import create_client, Client

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
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    position: Optional[str] = None

class ContactDiscoveryResult(BaseModel):
    domain: str
    emails_found: List[EmployeeEmail] = []

class CHCompanyData(BaseModel):
    company_name: str
    company_number: Optional[str] = None
    registered_address: Optional[str] = None
    directors: List[Dict[str, str]] = []

class PipelineJob(BaseModel):
    id: str
    status: str
    created_at: float
    updated_at: float
    total_companies_in: int = 0
    domains_discovered_count: int = 0
    total_emails_found_count: int = 0
    final_combined_results: List[Dict[str, Any]] = []

class BulkEmailJob(BaseModel):
    id: str
    status: str
    created_at: float
    updated_at: float
    total_contacts_uploaded: int = 0
    emails_sent_count: int = 0
    emails_skipped_count: int = 0
    errors: List[str] = []

# --- 2. Initialize FastAPI Application ---

app = FastAPI(title="ECA Pipeline & Sender API", description="Lead Enrichment & Bulk Email Sender")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 3. Persistent Data Mock & Supabase Blacklist ---
pipeline_jobs: Dict[str, PipelineJob] = {}
email_jobs: Dict[str, BulkEmailJob] = {}

# Initialise API Keys dynamically from Render's Environment Variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SERPER_API_KEY = os.environ.get("SERPER_API_KEY")
CH_API_KEY = os.environ.get("CH_API_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
else:
    logger.warning("Supabase credentials missing. Blacklist will not be enforced.")

def load_blacklist() -> set:
    """Fetches the global unsubscribe list from the cloud database."""
    if not supabase:
        return set()
    try:
        response = supabase.table("blacklist").select("email").execute()
        return set(row["email"] for row in response.data)
    except Exception as e:
        logger.error(f"Failed to load blacklist from Supabase: {e}")
        return set()

def add_to_blacklist(email: str):
    """Upserts an unsubscribed email into the cloud database."""
    if not supabase or not email:
        return
    try:
        clean_email = email.strip().lower()
        supabase.table("blacklist").upsert({"email": clean_email}).execute()
    except Exception as e:
        logger.error(f"Failed to add {email} to Supabase blacklist: {e}")

# --- 4. API Endpoints ---

@app.get("/")
async def root():
    return {"message": "ECA API is operational."}

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

# --- TOOL 1: THE DATA ENRICHER ---
@app.post("/upload-leads/", response_model=PipelineJob)
async def upload_leads_csv_start_pipeline(
    file: UploadFile = File(...),
    hunter_api_key: Optional[str] = Form(None),
    max_contacts_per_domain: int = Form(10),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    if not file.filename.lower().endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    file_bytes_captured = await file.read()
    job_id = f"eca_job_{uuid.uuid4().hex[:8]}"

    new_job = PipelineJob(id=job_id, status="Processing", created_at=time.time(), updated_at=time.time())
    pipeline_jobs[job_id] = new_job

    background_tasks.add_task(
        execute_full_pipeline, job_id, file_bytes_captured, file.filename,
        SERPER_API_KEY, CH_API_KEY, hunter_api_key, max_contacts_per_domain
    )
    return new_job

@app.get("/pipeline-status/{job_id}", response_model=PipelineJob)
async def get_pipeline_job_status(job_id: str):
    if job_id not in pipeline_jobs:
        raise HTTPException(status_code=404, detail="Job not found.")
    return pipeline_jobs[job_id]


# --- TOOL 2: THE BULK SENDER ---
@app.post("/bulk-send/", response_model=BulkEmailJob)
async def bulk_send_emails(
    file: UploadFile = File(...),
    smtp_user: str = Form(...),
    smtp_pass: str = Form(...),
    email_subject: str = Form(...),
    email_body: str = Form(...),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    if not file.filename.lower().endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    file_bytes_captured = await file.read()
    job_id = f"email_job_{uuid.uuid4().hex[:8]}"

    new_job = BulkEmailJob(id=job_id, status="Processing", created_at=time.time(), updated_at=time.time())
    email_jobs[job_id] = new_job

    background_tasks.add_task(
        execute_bulk_send, job_id, file_bytes_captured, file.filename, smtp_user, smtp_pass, email_subject, email_body
    )
    return new_job

@app.get("/bulk-send-status/{job_id}", response_model=BulkEmailJob)
async def get_bulk_send_status(job_id: str):
    if job_id not in email_jobs:
        raise HTTPException(status_code=404, detail="Email Job not found.")
    return email_jobs[job_id]


# --- 5. Background Workers ---

# Worker 1: The Enricher
async def execute_full_pipeline(job_id, file_bytes, filename, serper_api_key, ch_api_key, hunter_api_key, max_contacts):
    job = pipeline_jobs[job_id]
    try:
        target_company_names, company_metadata = await preprocess_uploaded_file(job, file_bytes, filename)
        async with httpx.AsyncClient(timeout=30.0) as client:
            domain_results = await run_serper_domain_discovery(job, client, target_company_names, serper_api_key) if serper_api_key else []
            ch_results = await run_companies_house_enrichment(job, client, target_company_names, ch_api_key) if ch_api_key else []
            contact_results = await run_hunter_contact_discovery(job, client, domain_results, hunter_api_key, max_contacts) if hunter_api_key else []
            await combine_and_finalize_results(job, target_company_names, domain_results, contact_results, ch_results, company_metadata)
    except Exception as e:
        logger.error(f"Pipeline error {job_id}: {e}")
        job.status = "Error"
    finally:
        if job.status != "Error": job.status = "Complete"
        job.updated_at = time.time()

# Worker 2: The Email Sender
async def execute_bulk_send(job_id: str, file_bytes: bytes, filename: str, smtp_user: str, smtp_pass: str, email_subject: str, email_body: str):
    job = email_jobs[job_id]
    logger.info(f"Starting Bulk Send for Job {job_id}")

    try:
        if filename.lower().endswith('.csv'):
            try:
                df = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
            except UnicodeDecodeError:
                df = pd.read_csv(io.StringIO(file_bytes.decode('iso-8859-1')))
        else:
            df = pd.read_excel(io.BytesIO(file_bytes))

        job.total_contacts_uploaded = len(df)

        blacklist = load_blacklist()
        base_url = os.environ.get("RENDER_EXTERNAL_URL", "http://127.0.0.1:8000")

        # Convert {variables} to Jinja2 {{ variables }} format
        converted_body = re.sub(r'(?<!\{)\{([a-zA-Z0-9_]+)\}(?!\})', r'{{\1}}', email_body)
        template_str = f"""
        {converted_body}
        <br><br>
        <p style="font-size: 10px; color: gray;">
            If you no longer wish to receive emails from me, <a href="{base_url}/unsubscribe?email={{{{ email }}}}">click here to unsubscribe</a>.
        </p>
        """
        template_str = template_str.replace('\n', '<br>')
        jinja_template = Template(template_str)

        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(smtp_user, smtp_pass)

        for index, row in df.iterrows():
            target_email = str(row.get("Employee Email", "")).strip().lower()
            company_name = str(row.get("Target Company Name", "your company")).strip()
            employee_name = str(row.get("Employee Name", "there")).strip()

            prop_addr = str(row.get("Property Address", "your property")).strip()
            if prop_addr == "nan": prop_addr = "your property"

            purch_year = str(row.get("Purchase Year", "recently")).strip()
            if purch_year == "nan": purch_year = "recently"

            if not target_email or "none found" in target_email or target_email == "nan" or target_email in blacklist:
                job.emails_skipped_count += 1
                continue

            first_name = employee_name.split()[0] if employee_name and employee_name != "UNIDENTIFIED" else "there"

            html_body = jinja_template.render(
                director_first_name=first_name,
                company=company_name,
                property_address=prop_addr,
                purchase_year=purch_year,
                email=target_email
            )

            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = target_email
            msg['Subject'] = email_subject
            msg.attach(MIMEText(html_body, 'html'))

            try:
                server.send_message(msg)
                job.emails_sent_count += 1
                await asyncio.sleep(2)
            except Exception as e:
                job.errors.append(f"Failed to send to {target_email}: {str(e)}")

        server.quit()
    except Exception as e:
        logger.error(f"Bulk send error {job_id}: {e}")
        job.status = "Error"
        job.errors.append(str(e))
    finally:
        if job.status != "Error": job.status = "Complete"
        job.updated_at = time.time()
        logger.info(f"Bulk Send {job_id} complete. Sent {job.emails_sent_count}.")


# --- 6. Helper Functions (Enrichment Logic) ---
async def preprocess_uploaded_file(job: PipelineJob, file_contents: bytes, filename: str) -> Tuple[List[str], Dict[str, Any]]:
    if filename.lower().endswith('.csv'):
        try:
            df = pd.read_csv(io.StringIO(file_contents.decode('utf-8')))
        except:
            df = pd.read_csv(io.StringIO(file_contents.decode('iso-8859-1')))
    else:
        df = pd.read_excel(io.BytesIO(file_contents))

    cols = ['Proprietor Name (1)', 'Recipient Name', 'company_name', 'company', 'name']
    found_col = next((c for c in df.columns if c.lower() in [x.lower() for x in cols]), None)
    if not found_col: raise HTTPException(status_code=400, detail="Company column not found.")

    # Extract Property Address and Convert Excel Date format to Year
    if 'Date Proprietor Added' in df.columns:
        df['Purchase Year'] = pd.to_datetime(pd.to_numeric(df['Date Proprietor Added'], errors='coerce'), unit='D', origin='1899-12-30').dt.year
        df['Purchase Year'] = df['Purchase Year'].fillna('Unknown').astype(str).str.replace('.0', '', regex=False)
    else:
        df['Purchase Year'] = 'Unknown'

    company_metadata = {}
    for _, row in df.iterrows():
        comp_name = str(row[found_col]).strip()
        if comp_name and comp_name != 'nan' and comp_name not in company_metadata:
            company_metadata[comp_name] = {
                'Property Address': str(row.get('Property Address', 'your property')),
                'Purchase Year': str(row.get('Purchase Year', 'recently'))
            }

    names = list(company_metadata.keys())
    job.total_companies_in = len(names)
    return names, company_metadata

async def run_serper_domain_discovery(job, client, company_names, api_key):
    results, discovered = [], set()
    for name in company_names:
        search_name = re.sub(r'[^a-zA-Z0-9\s]', '', name).strip()
        payload = {"q": f"{search_name} UK website", "gl": "gb"}
        domain = None
        try:
            res = await client.post("https://google.serper.dev/search", headers={'X-API-KEY': api_key}, json=payload)
            if res.status_code == 200:
                for item in res.json().get('organic', []):
                    netloc = urlparse(item.get('link', '')).netloc.lower()
                    dirs = ['.gov.uk', '.linkedin.com', 'companieshouse', 'endole', 'yell', 'zoominfo']
                    if any(d in netloc for d in dirs): continue

                    raw_dom = netloc.replace('www.', '')
                    clean_dom = re.sub(r'[^a-z0-9]', '', raw_dom.split('.')[0])
                    clean_nm = re.sub(r'[^a-zA-Z0-9\s]', '', name.replace('&',' and ')).lower()
                    tokens = [w for w in clean_nm.split() if w not in {'limited','ltd','uk','plc','group','co','and'}]

                    if tokens:
                        matches = [t for t in tokens if t in clean_dom]
                        if not matches: continue
                        if not any(len(t)>=4 for t in matches):
                            chars = sum(len(t) for t in matches)
                            for sw in ['ltd','group','uk','co']:
                                if clean_dom.endswith(sw): chars += len(sw)
                            if (len(clean_dom) > 0) and (chars/len(clean_dom) < 0.4): continue
                    domain = raw_dom
                    break
        except: pass
        if domain and domain not in discovered:
            results.append(DomainInfo(provided_name=name, discovered_domain=domain))
            discovered.add(domain)
        else:
            results.append(DomainInfo(provided_name=name))
    job.domains_discovered_count = len(discovered)
    return results

async def run_companies_house_enrichment(job, client, company_names, api_key):
    results = []
    auth = (api_key, "")
    for name in company_names:
        info = CHCompanyData(company_name=name)
        try:
            res = await client.get(f"https://api.company-information.service.gov.uk/search/companies?q={quote_plus(name)}", auth=auth)
            if res.status_code == 200 and res.json().get("items"):
                top = res.json()["items"][0]
                info.company_number = top.get("company_number")
                addr = top.get("address", {})
                info.registered_address = ", ".join([addr.get(k) for k in ["address_line_1", "locality", "postal_code"] if addr.get(k)])

                if info.company_number:
                    off_res = await client.get(f"https://api.company-information.service.gov.uk/company/{info.company_number}/officers", auth=auth)
                    if off_res.status_code == 200:
                        info.directors = [{"name": o.get("name"), "role": o.get("officer_role", "Director").title()}
                                          for o in off_res.json().get("items", [])
                                          if o.get("officer_role") in ["director", "nominee-director"] and not o.get("resigned_on")]
        except: pass
        results.append(info)
    return results

async def run_hunter_contact_discovery(job, client, discovery_results, api_key, max_per_domain):
    results, total = [], 0
    domains = [r.discovered_domain for r in discovery_results if r.discovered_domain]
    blocks = ('info@', 'admin@', 'customercare@', 'customer.service@', 'sales@', 'support@', 'contact@')

    for domain in domains:
        res_obj = ContactDiscoveryResult(domain=domain)
        try:
            res = await client.get(f"https://api.hunter.io/v2/domain-search?domain={quote_plus(domain)}&limit={max_per_domain}&api_key={api_key}")
            if res.status_code == 200:
                emails = []
                for e in res.json().get('data', {}).get('emails', []):
                    val = e.get('value','').lower()
                    if not any(val.startswith(p) for p in blocks):
                        emails.append(EmployeeEmail(email=val, type=e.get('type','personal'), first_name=e.get('first_name'), last_name=e.get('last_name'), position=e.get('position')))
                res_obj.emails_found = emails
                total += len(emails)
        except: pass
        results.append(res_obj)
    job.total_emails_found_count = total
    return results

async def combine_and_finalize_results(job, company_names, domains, contacts, ch_data, company_metadata):
    combined = []
    dom_map = {r.provided_name: r.discovered_domain for r in domains}
    con_map = {r.domain: r.emails_found for r in contacts}
    ch_map = {c.company_name: c for c in ch_data}

    for name in company_names:
        domain = dom_map.get(name)
        emails = con_map.get(domain) if domain else None
        ch = ch_map.get(name)
        meta = company_metadata.get(name, {})

        ch_num = ch.company_number if ch else "NOT FOUND"
        ch_addr = ch.registered_address if ch else "NOT FOUND"
        director = ch.directors[0]["name"] if (ch and ch.directors) else "UNIDENTIFIED"
        prop_addr = meta.get('Property Address', 'your property')
        purch_year = meta.get('Purchase Year', 'recently')

        base_dict = {
            "Target Company Name": name,
            "Property Address": prop_addr,
            "Purchase Year": purch_year,
            "Companies House Number": ch_num,
            "Registered Address": ch_addr,
        }

        if domain and emails:
            for e in emails:
                person = f"{e.first_name} {e.last_name}" if e.first_name else director
                row = base_dict.copy()
                row.update({"Discovered Domain": domain, "Employee Name": person, "Employee Position": e.position or "Director", "Employee Email": e.email, "Status": "Complete (Email Found)"})
                combined.append(row)
        elif domain and not emails:
            row = base_dict.copy()
            row.update({"Discovered Domain": domain, "Employee Name": director, "Employee Position": "Director (CH)", "Employee Email": "NONE FOUND (Hunter)", "Status": "Domain Found (No Email)"})
            combined.append(row)
        else:
            row = base_dict.copy()
            row.update({"Discovered Domain": "NONE FOUND (Serper)", "Employee Name": director, "Employee Position": "Director (CH)", "Employee Email": "NONE FOUND (SPV)", "Status": "SPV Fallback (CH)"})
            combined.append(row)

    job.final_combined_results = combined


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info")
