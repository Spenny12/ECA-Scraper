import os
import io
import time
import logging
import asyncio
import uuid
import re
from typing import List, Dict, Optional, Any, Set
from pydantic import BaseModel, Field

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
from urllib.parse import quote_plus, urlparse

# --- 0. Set Up Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger("ECA_Pipeline")

# --- 1. Define Core Objects and Data Structures ---

class DomainInfo(BaseModel):
    provided_name: str
    discovered_domain: Optional[str] = None
    discovery_method: Optional[str] = None  # e.g., 'GoogleSearch', 'GoogleMap', 'Fallback'
    search_query_used: Optional[str] = None

class EmployeeEmail(BaseModel):
    email: str
    type: str  # generic, personal
    verification: Optional[str] = None # e.g., 'deliverable', 'risky'
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    position: Optional[str] = None
    source: Optional[str] = None

class ContactDiscoveryResult(BaseModel):
    domain: str
    emails_found: List[EmployeeEmail] = []
    hunter_score: Optional[float] = None
    organization_name: Optional[str] = None

class FullEnrichProfile(BaseModel):
    linkedin_url: Optional[str] = None
    # We will accept any fields FullEnrich returns, focusing on LinkedIn.
    profile_data: Dict[str, Any] = {}

class FullEnrichResult(BaseModel):
    source_email: str
    linkedin_profile: Optional[FullEnrichProfile] = None
    enrichment_status: str # e.g., 'Success', 'No Profile Found', 'Skipped', 'Error'

class PipelineJob(BaseModel):
    id: str
    status: str  # e.g., 'Processing', 'Complete', 'Error'
    created_at: float
    updated_at: float
    # API Keys used (masks actual keys for safety)
    used_google: bool = False
    used_companies_house: bool = False
    used_hunter: bool = False
    used_fullenrich: bool = False # Optional, track if used

    # Progress trackers
    total_companies_in: int = 0
    domains_discovered_count: int = 0
    domains_with_emails_count: int = 0
    total_emails_found_count: int = 0
    emails_sent_for_enrichment_count: int = 0
    linkedin_profiles_found_count: int = 0

    # Data results (simplified for response, detailed internal use)
    discovered_domains: List[Dict[str, str]] = []
    discovered_contacts: List[Dict[str, Any]] = []
    fullenrich_matches: List[Dict[str, Any]] = []

    final_combined_results: List[Dict[str, Any]] = []

# --- 2. Initialize FastAPI Application ---

app = FastAPI(title="ECA Cold Email Pipeline", description="Lead Enrichment & LinkedIn Discovery API")

# Configure CORS for local development (adjust origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# --- 3. Persistent Data Mock ---
# In production, use a Database (PostgreSQL/MongoDB) or Redis cache.
pipeline_jobs: Dict[str, PipelineJob] = {}

# --- 4. Define API Endpoints ---

@app.get("/")
async def root():
    return {"message": "ECA Pipeline API is operational."}

# endpoint 1: /upload-leads/
@app.post("/upload-leads/", response_model=PipelineJob)
async def upload_leads_csv_start_pipeline(
    file: UploadFile = File(...),
    # Optional API Keys from user
    google_api_key: Optional[str] = Form(None),
    google_cx_string: Optional[str] = Form(None),
    ch_api_key: Optional[str] = Form(None),
    hunter_api_key: Optional[str] = Form(None),
    fullenrich_api_key: Optional[str] = Form(None), # Optional FullEnrich
    # Configuration
    max_contacts_per_domain: int = Form(10),
    hunter_email_verification: bool = Form(True),
    background_tasks: BackgroundTasks = BackgroundTasks() #
):
    """
    Accepts CSV upload of target company names, starts the backend asynchronous pipeline,
    and returns a unique job ID immediately.
    """
    if file.filename.endswith(('.csv', '.xlsx', '.xls')) is False:
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload CSV or Excel.")

    # 1. Create a unique ID and initialized Job record
    job_id = f"eca_job_{uuid.uuid4().hex[:8]}"
    current_time = time.time()

    # Initialize optional key flags
    used_fe = True if (fullenrich_api_key and len(fullenrich_api_key) > 5) else False

    new_job = PipelineJob(
        id=job_id,
        status="Processing",
        created_at=current_time,
        updated_at=current_time,
        used_google=True if google_api_key else False,
        used_companies_house=True if ch_api_key else False,
        used_hunter=True if hunter_api_key else False,
        used_fullenrich=used_fe
    )
    pipeline_jobs[job_id] = new_job
    logger.info(f"Initialized job {job_id}. Status: {new_job.status}")

    # 2. Add the pipeline execution to background tasks
    background_tasks.add_task(
        execute_full_pipeline,
        job_id,
        file,
        google_api_key, google_cx_string,
        ch_api_key,
        hunter_api_key,
        fullenrich_api_key, # Mandatory argument, but may be None
        max_contacts_per_domain,
        hunter_email_verification
    )

    # 3. Return the job tracker immediately
    return new_job

# endpoint 2: /pipeline-status/{job_id}
@app.get("/pipeline-status/{job_id}", response_model=PipelineJob)
async def get_pipeline_job_status(job_id: str):
    """
    Returns the current status, progress, and results of a given pipeline job.
    """
    job_record = pipeline_jobs.get(job_id)
    if not job_record:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
    return job_record

# endpoint 3: /pipeline-results-csv/{job_id}
# (Placeholder for CSV download logic - would generate CSV and return response)
@app.get("/pipeline-results-csv/{job_id}")
async def download_combined_results_csv(job_id: str):
    # Retrieve job, generate DataFrame from final_combined_results,
    # Convert DataFrame to CSV/BytesIO and return streaming response.
    # Logic similar to how results are displayed/downloaded from UI.
    return {"message": "Placeholder for generated CSV output.", "job_id": job_id}

# --- 5. Full Asynchronous Pipeline Logic (Background Task) ---

async def execute_full_pipeline(
    job_id: str,
    file_upload: UploadFile,
    google_api_key: Optional[str],
    google_cx_string: Optional[str],
    ch_api_key: Optional[str],
    hunter_api_key: Optional[str],
    fullenrich_api_key: Optional[str], # Can be None
    max_contacts_per_domain: int,
    hunter_email_verification: bool
):
    """
    The main asynchronous function that coordinates all pipeline steps.
    """
    logger.info(f"Background Pipeline Starting for Job: {job_id}")
    job = pipeline_jobs[job_id]

    # Optional Key Check (Log but continue if missing)
    if not fullenrich_api_key:
        logger.warning(f"Job {job_id}: No FullEnrich API key provided. LinkedIn discovery will be skipped.")

    try:
        # Step A: Pre-processing (Read File, Extract Names)
        target_company_names = await preprocess_uploaded_file(job, file_upload)

        # Async HTTP Client for reusable connections
        async with httpx.AsyncClient(timeout=30.0) as client:

            # Step B: Google Domain Discovery
            if job.used_google and google_api_key and google_cx_string:
                domain_discovery_results = await run_google_domain_discovery(
                    job, client, target_company_names, google_api_key, google_cx_string
                )
            else:
                logger.warning(f"Job {job_id}: Skipping Google discovery. Key or CX missing.")
                domain_discovery_results = [] # Continue with empty list, or fallback needed

            # Step C: Optional Companies House Verification
            # (If domains found are wrong/service.gov.uk, could attempt fallback search)
            # This is complex fallback logic, placeholder for integration.

            # Step D: Hunter.io Contact Discovery
            if job.used_hunter and hunter_api_key and domain_discovery_results:
                discovered_contacts = await run_hunter_contact_discovery(
                    job, client, domain_discovery_results, hunter_api_key, max_contacts_per_domain, hunter_email_verification
                )
            else:
                logger.warning(f"Job {job_id}: Skipping Hunter.io discovery. Key missing or no domains found.")
                discovered_contacts = []

            # Step E: FullEnrich LinkedIn Discovery (Optional)
            if job.used_fullenrich and fullenrich_api_key and discovered_contacts:
                enrichment_results = await run_fullenrich_linkedin_discovery(
                    job, client, discovered_contacts, fullenrich_api_key
                )
            else:
                if job.used_fullenrich and not fullenrich_api_key:
                    logger.info(f"Job {job_id}: FullEnrich was marked as optional/skipped. Proceeding to finalize.")
                else:
                    logger.warning(f"Job {job_id}: Skipping FullEnrich. Key missing or no contacts found.")
                enrichment_results = []

            # Step F: Final Data Combination & Simplification
            await combine_and_finalize_results(job, target_company_names, domain_discovery_results, discovered_contacts, enrichment_results)

    except Exception as e:
        logger.error(f"Critical error in pipeline job {job_id}: {str(e)}")
        job.status = "Error"
        # Optional: Add error detail to a job error log attribute.

    finally:
        if job.status != "Error":
            job.status = "Complete"
        job.updated_at = time.time()
        logger.info(f"Background Pipeline Finished for Job: {job_id}. Status: {job.status}")


# --- 6. Pipeline Step Worker Functions ---

# --- A. Preprocessing ---
async def preprocess_uploaded_file(job: PipelineJob, file_upload: UploadFile) -> List[str]:
    logger.info(f"Job {job.id}: Starting preprocessing.")
    contents = await file_upload.read()

    try:
        # Load file contents into Pandas
        if file_upload.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents))
        else: # Excel
            df = pd.read_excel(io.BytesIO(contents))

        logger.info(f"Parsed DataFrame with shape: {df.shape}")
        # Assuming the CSV contains a column named 'company_name' or similar.
        # Add flexibility to detect potential company column names.
        potential_company_cols = ['company', 'company_name', 'name', 'target', 'business_name']
        found_col = next((col for col in df.columns if col.lower() in potential_company_cols), None)

        if not found_col:
             raise HTTPException(status_code=400, detail=f"Required column 'company_name' (or similar like 'company') not found in upload. Columns found: {list(df.columns)}")

        # Extract and clean names, dropping duplicates/empties
        raw_names = df[found_col].dropna().unique()
        clean_company_names = [str(name).strip() for name in raw_names if str(name).strip()]

        logger.info(f"Job {job.id}: Preprocessing complete. Identified {len(clean_company_names)} unique target company names.")
        job.total_companies_in = len(clean_company_names)
        job.updated_at = time.time()
        return clean_company_names

    except pd.errors.ParserError as e:
        logger.error(f"ParserError during preprocessing: {e}")
        raise HTTPException(status_code=400, detail="Error parsing the uploaded CSV file. Ensure it is well-formatted.")
    except Exception as e:
        logger.error(f"General preprocessing error: {e}")
        raise

# --- B. Google Domain Discovery ---
async def run_google_domain_discovery(
    job: PipelineJob,
    client: httpx.AsyncClient,
    company_names: List[str],
    api_key: str,
    cx_string: str
) -> List[DomainInfo]:
    logger.info(f"Job {job.id}: Starting Google Domain Discovery.")
    results = []

    # Track the domains already found to avoid duplicates for the same query.
    discovered_domains_set: Set[str] = set()

    for name in company_names:
        # 1. Clean the company name for optimal UK search
        search_name = re.sub(r'[^a-zA-Z0-9\s]', '', name) # Remove special chars
        # Important UK suffix to target UK businesses in general search
        query = quote_plus(f"{search_name} UK")

        # 2. Perform the Google Search API Request
        # (This combines the API Key and the Search Engine ID `cx`)
        google_url = f"https://www.googleapis.com/customsearch/v1?key={api_key}&cx={cx_string}&q={query}"

        discovered_domain = None
        discovery_method = 'GoogleSearch'

        try:
            response = await client.get(google_url)
            # In a real environment, you must handle API errors (e.g., 429 quota, 400 bad key)
            if response.status_code == 200:
                search_data = response.json()

                # Iterate through search results looking for a likely business homepage
                if 'items' in search_data and len(search_data['items']) > 0:
                    for item in search_data['items']:
                        item_link = item.get('link')
                        if not item_link: continue

                        parsed_url = urlparse(item_link)
                        # We must exclude gov.uk domains that are not business homepages
                        # And potentially exclude massive directories like LinkedIn profiles (unless needed).
                        netloc = parsed_url.netloc.lower()
                        if '.gov.uk' not in netloc and '.linkedin.com/company/' not in netloc:
                            # We found a potential business homepage domain!
                            # Extract the root domain (e.g., company.co.uk)
                            raw_domain = netloc.replace('www.', '')
                            discovered_domain = raw_domain
                            break # Found the first suitable domain
            else:
                 logger.error(f"Google API Error {response.status_code}: {response.text}")

        except httpx.RequestError as e:
            logger.error(f"HTTP Request error during Google Search: {e}")

        # 3. Add result, even if failed, for final accounting
        if discovered_domain:
            # Avoid repeating discovery of the exact same domain for the same query set.
            if discovered_domain not in discovered_domains_set:
                info = DomainInfo(
                    provided_name=name,
                    discovered_domain=discovered_domain,
                    discovery_method=discovery_method,
                    search_query_used=query
                )
                results.append(info)
                discovered_domains_set.add(discovered_domain)
                logger.info(f"Discovered domain '{discovered_domain}' for target '{name}'")
            else:
                logger.info(f"Duplicate domain '{discovered_domain}' found for query '{name}'. Skipping recording duplicate.")
        else:
             info = DomainInfo(provided_name=name, discovered_domain=None) # No domain found
             results.append(info)

    # Update job tracker
    job.domains_discovered_count = len(discovered_domains_set)
    # Convert result objects to simplified dicts for status response
    job.discovered_domains = [{"target": r.provided_name, "domain": r.discovered_domain if r.discovered_domain else "NONE FOUND"} for r in results if r.discovered_domain]
    job.updated_at = time.time()
    logger.info(f"Job {job.id}: Domain Discovery complete. Found {job.domains_discovered_count} unique domains.")
    return results

# --- D. Hunter.io Contact Discovery ---
async def run_hunter_contact_discovery(
    job: PipelineJob,
    client: httpx.AsyncClient,
    discovery_results: List[DomainInfo],
    api_key: str,
    max_per_domain: int,
    verification: bool
) -> List[ContactDiscoveryResult]:
    logger.info(f"Job {job.id}: Starting Hunter.io Contact Discovery.")
    results = []

    total_emails = 0
    domains_processed_with_contacts = 0

    # We only process entries where a domain was successfully discovered.
    domains_to_process = [r.discovered_domain for r in discovery_results if r.discovered_domain]

    for domain in domains_to_process:
        logger.info(f"Querying Hunter.io for domain: {domain}")

        # 1. Prepare Hunter.io API Call (using the discovered root domain)
        # `query` is the parameter for the domain, e.g., google.com
        hunter_url = f"https://api.hunter.io/v2/domain-search?domain={quote_plus(domain)}&limit={max_per_domain}&api_key={api_key}"

        discovery_result = ContactDiscoveryResult(domain=domain)

        try:
            response = await client.get(hunter_url)
            if response.status_code == 200:
                hunter_data = response.json().get('data', {})

                # Extract meta-data and emails
                emails_found = hunter_data.get('emails', [])
                if emails_found:
                    discovery_result.organization_name = hunter_data.get('organization')
                    discovery_result.hunter_score = hunter_data.get('score')

                    # Convert raw data into structured EmployeeEmail objects
                    structured_emails = []
                    for e in emails_found:
                        employee = EmployeeEmail(
                            email=e.get('value'),
                            type=e.get('type'),
                            first_name=e.get('first_name'),
                            last_name=e.get('last_name'),
                            position=e.get('position')
                            # Optional: Verification result can be added here if needed
                        )
                        structured_emails.append(employee)

                    discovery_result.emails_found = structured_emails
                    total_emails += len(structured_emails)
                    domains_processed_with_contacts += 1

            else:
                 logger.error(f"Hunter API Error {response.status_code}: {response.text}")

        except httpx.RequestError as e:
            logger.error(f"HTTP Request error during Hunter.io Search: {e}")

        # 3. Store result even if empty
        results.append(discovery_result)

    # Update job tracker
    job.total_emails_found_count = total_emails
    job.domains_with_emails_count = domains_processed_with_contacts

    # Convert objects to simple dicts for status response
    job.discovered_contacts = []
    for r in results:
        for e in r.emails_found:
            job.discovered_contacts.append({
                "domain": r.domain,
                "email": e.email,
                "name": f"{e.first_name} {e.last_name}" if (e.first_name and e.last_name) else "UNIDENTIFIED",
                "position": e.position if e.position else "UNIDENTIFIED",
                "org": r.organization_name
            })

    job.updated_at = time.time()
    logger.info(f"Job {job.id}: Contact Discovery complete. Found {job.total_emails_found_count} emails across {job.domains_with_emails_count} domains.")
    return results

# --- E. FullEnrich LinkedIn Discovery (Optional) ---
async def run_fullenrich_linkedin_discovery(
    job: PipelineJob,
    client: httpx.AsyncClient,
    discovered_contacts: List[ContactDiscoveryResult],
    api_key: str
) -> List[FullEnrichResult]:
    logger.info(f"Job {job.id}: Starting Optional FullEnrich LinkedIn Discovery.")
    results = []

    # 1. Flatten the list of all discovered emails to enrich
    all_emails_to_enrich = []
    for contact_result in discovered_contacts:
        for employee_email in contact_result.emails_found:
            all_emails_to_enrich.append(employee_email.email)

    total_emails_sent = len(all_emails_to_enrich)
    linkedin_found = 0
    job.emails_sent_for_enrichment_count = total_emails_sent
    job.updated_at = time.time()

    # If the user passed the optional key, we proceed. If it's missing (None), this loop shouldn't run based on execute_full_pipeline's check.
    # We make a final check here just in case.
    if not api_key:
        logger.info(f"Job {job.id}: Optional FullEnrich key is None. Skipping further checks.")
        return results # Or return special skipped entries.

    logger.info(f"Job {job.id}: Optional FullEnrich active. Processing {total_emails_sent} potential contacts for LinkedIn profiles.")

    # --- Integration with FullEnrich Batch API ---
    # FullEnrich typically uses a two-step batch process: Create Request, then Get Results.

    fullenrich_base_url = "https://api.fullenrich.com/v1" # Example Base URL

    # Step E1: Create the Batch Enrichment Request
    request_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    request_body = {
        "emails": all_emails_to_enrich,
        # Potentially specify needed fields, like 'linkedin_profile'
        "required_fields": ["linkedin_profile"]
    }

    try:
        # Create request response (Example: returns a 'batch_id')
        # response = await client.post(f"{fullenrich_base_url}/batch/create", headers=request_headers, json=request_body)

        # MOCKING Step E1 and E2 for prototype purposes:
        logger.info(f"MOCK: FullEnrich batch enrichment requested for {total_emails_sent} emails using provided Optional Key.")
        # and would return successful enrichment profiles

        # We will mock the enrichment result of a hypothetical successful match.
        # This replaces real API calls that would be configured here using the provided key.
        # It shows how the data structure handles the successful return from the optional stage.
        mock_fullenrich_return_profile = FullEnrichProfile(
             linkedin_url="https://www.linkedin.com/in/john.doe-123",
             profile_data={
                 "name": "John Doe",
                 "headline": "Example CEO | Software Solutions UK",
                 "company": "Software Solutions Limited",
                 "experience": [{"title": "CEO", "company": "Software Solutions"}]
                 }
        )
        # Assuming the MOCK finds a match for one hypothetical email for testing.
        match_made = False

        # Iterate and build results (Mocking successful profile found if the logic returns matches)
        for email in all_emails_to_enrich:

            enrichment_status = 'Skipped (Optional)' # By default assume it was skipped because key is checked optional.
            profile_found = None

            # --- Check optional key and process if possible (MOCK LOGIC) ---
            if api_key: # Key exists (optional is active)
                # Real API would populate this successful result profile.
                if not match_made: # Mocking one successful profile match
                    profile_found = mock_fullenrich_return_profile
                    linkedin_found += 1
                    match_made = True
                    enrichment_status = 'Success'
                    logger.info(f"MOCK FullEnrich: LinkedIn Profile found for {email}")
                else:
                    enrichment_status = 'No Profile Found' # Profile wasn't located by API

            # Record the result object
            res = FullEnrichResult(
                source_email=email,
                linkedin_profile=profile_found,
                enrichment_status=enrichment_status
            )
            results.append(res)

    except Exception as e:
        logger.error(f"HTTP Request error during FullEnrich Step: {e}")
        job.status = "Error (FullEnrich)"
        raise # Rethrow critical error

    # Update job tracker based on final match counts
    job.linkedin_profiles_found_count = linkedin_found

    # Convert objects to simple dicts for status response
    job.fullenrich_matches = []
    for r in results:
        if r.linkedin_profile:
            job.fullenrich_matches.append({
                "email": r.source_email,
                "linkedin": r.linkedin_profile.linkedin_url,
                "headline": r.linkedin_profile.profile_data.get('headline')
            })

    job.updated_at = time.time()
    logger.info(f"Job {job.id}: Optional FullEnrich complete. Found {job.linkedin_profiles_found_count} matched LinkedIn profiles using provided key.")
    return results

# --- F. Final Data Combination ---
async def combine_and_finalize_results(
    job: PipelineJob,
    company_names: List[str],
    domains: List[DomainInfo],
    contacts: List[ContactDiscoveryResult],
    enrichments: List[FullEnrichResult]
):
    logger.info(f"Job {job.id}: Starting final data combination.")
    combined_results = []

    # Flatten structure and map results back to the original company names and domains

    # 1. Flatten discovery results for mapping target->domain
    domain_map = {r.provided_name: (r.discovered_domain if r.discovered_domain else None) for r in domains}

    # 2. Flatten contact discovery for mapping domain->emails list
    contact_map = {r.domain: r.emails_found for r in contacts}

    # 3. Flatten enrichment results for mapping email->LinkedIn/Profile
    enrichment_map = {r.source_email: r.linkedin_profile for r in enrichments if r.linkedin_profile}
    enrichment_status_map = {r.source_email: r.enrichment_status for r in enrichments}

    # Iterate through each original company target to create output rows
    for company_name in company_names:

        discovered_domain = domain_map.get(company_name)
        emails = contact_map.get(discovered_domain) if discovered_domain else None

        if not discovered_domain:
            # Handle companies with no found domain
            row = {
                "Target Company Name": company_name,
                "Discovered Domain": "NONE FOUND (Google)",
                "Employee Name": "UNIDENTIFIED",
                "Employee Position": "UNIDENTIFIED",
                "Employee Email": "UNIDENTIFIED",
                "LinkedIn URL": "SKIPPED",
                "FullEnrich Profile": "SKIPPED",
                "Discovery Pipeline Status": "Domain Failed"
            }
            combined_results.append(row)
            continue # Move to next company

        if not emails:
             # Handle domain found but no contacts
             row = {
                "Target Company Name": company_name,
                "Discovered Domain": discovered_domain,
                "Employee Name": "UNIDENTIFIED",
                "Employee Position": "UNIDENTIFIED",
                "Employee Email": "NONE FOUND (Hunter)",
                "LinkedIn URL": "SKIPPED",
                "FullEnrich Profile": "SKIPPED",
                "Discovery Pipeline Status": "Contacts Failed"
            }
             combined_results.append(row)
             continue # Move to next company

        # 4. Companies with domains AND contacts: create a row for each employee
        for email in emails:

            enrichment_profile = enrichment_map.get(email.email)
            enrichment_status = enrichment_status_map.get(email.email, "Pending/Skipped (Optional)")

            # Map enrichment_profile to a more useful description of what was found (LinkedIn vs generic vs skipped)
            full_enrich_status_summary = enrichment_status
            if enrichment_profile:
                if enrichment_profile.linkedin_url:
                    full_enrich_status_summary = f"Success (LinkedIn Matched: {enrichment_profile.linkedin_url})"
                elif enrichment_profile.profile_data:
                    full_enrich_status_summary = "Success (Generic Profile/Waitlist)"

            # If FullEnrich was marked as optional/skipped because of no key, summarize this.
            if enrichment_status == "Skipped (Optional)" and not job.used_fullenrich:
                 full_enrich_status_summary = "Skipped (FullEnrich Key Optional/Missing)"

            row = {
                "Target Company Name": company_name,
                "Discovered Domain": discovered_domain,
                "Employee Name": f"{email.first_name} {email.last_name}" if (email.first_name and email.last_name) else "UNIDENTIFIED",
                "Employee Position": email.position if email.position else "UNIDENTIFIED",
                "Employee Email": email.email,
                "LinkedIn URL": enrichment_profile.linkedin_url if (enrichment_profile and enrichment_profile.linkedin_url) else "NONE FOUND",
                "FullEnrich Profile": full_enrich_status_summary, # Contains the detailed success/skip summary
                "Discovery Pipeline Status": "Pipeline Complete"
            }
            combined_results.append(row)

    # Convert object to simplified JSON serializable lists/dicts
    job.final_combined_results = combined_results
    job.updated_at = time.time()
    logger.info(f"Job {job.id}: Final Data Combination complete. Generated {len(combined_results)} output rows across all companies.")

# --- 7. Main Execution Entry Point ---
# Run the FastAPI server using Uvicorn:
# uvicorn main:app --reload
# Access Interactive Documentation: http://127.0.0.1:8000/docs

if __name__ == "__main__":
    import uvicorn
    # Use the port specified in the ' vibecode' deployment examples or default 8000.
    # The --reload flag should be handled by the command line, not code when running via Python.
    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info")
