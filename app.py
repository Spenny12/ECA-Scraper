import io
import asyncio
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import httpx

app = FastAPI(title="ECA Scraper Prototype")

# Enable CORS for Next.js frontend local server communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global in-memory data store to manage background task progress states
jobs = {}

async def execute_enrichment_pipeline(
    job_id: str, 
    df: pd.DataFrame, 
    ch_key: str, 
    google_key: str, 
    google_cx: str, 
    hunter_key: str, 
    fullenrich_key: str, 
    mobile_cap: int
):
    """
    Asynchronous worker processing rows sequentially.
    Enforces quality gates and an explicit spending cap on premium lookups.
    """
    total_rows = len(df)
    jobs[job_id] = {"status": "processing", "progress": 0, "total": total_rows, "results": []}
    
    processed_leads = []
    mobile_lookup_counter = 0

    async with httpx.AsyncClient() as client:
        for index, row in df.iterrows():
            # Extract core variables derived from Land Registry dataset mapping
            company_name = str(row.get("Proprietor name", "")).strip()
            company_number = str(row.get("Company registered number", "")).strip()
            
            # Initialize default tracking values for enrichment columns
            domain = "N/A"
            director_first = "N/A"
            director_last = "N/A"
            email_address = "N/A"
            phone_number = "N/A"
            linkedin_url = "N/A"

            # Skip processing if critical identifiers are structurally absent
            if not company_name or company_name == "nan":
                jobs[job_id]["progress"] = index + 1
                continue

            try:
                # -------------------------------------------------------------
                # STAGE 1: Google Custom Search API Domain Discovery
                # -------------------------------------------------------------
                # Intent: Query name + unique registration code to target footers
                search_query = f"{company_name} {company_number}".strip()
                google_url = f"https://www.googleapis.com/customsearch/v1?key={google_key}&cx={google_cx}&q={search_query}"
                
                google_res = await client.get(google_url)
                if google_res.status_code == 200:
                    search_data = google_res.json()
                    items = search_data.get("items", [])
                    if items:
                        # Extract root URL path from the top organic search result
                        raw_url = items[0].get("link", "")
                        # Simple programmatic parsing to isolate base host domain string
                        if "://" in raw_url:
                            domain = raw_url.split("://")[1].split("/")[0].replace("www.", "")

                # -------------------------------------------------------------
                # STAGE 2: Companies House API Active Officer Assessment
                # -------------------------------------------------------------
                if company_number and company_number != "nan":
                    ch_url = f"https://api.company-information.service.gov.uk/company/{company_number}/officers"
                    # Basic authentication scheme requiring API key passed as the username config
                    ch_res = await client.get(ch_url, auth=(ch_key, ""))
                    
                    if ch_res.status_code == 200:
                        officers_data = ch_res.json()
                        active_officers = [
                            o for o in officers_data.get("items", []) 
                            if not o.get("resigned_on") and "director" in o.get("officer_role", "").lower()
                        ]
                        
                        # Apply business rule filter targeting youngest decision maker profiles
                        if active_officers:
                            # Parse birth parameters into an indexable value
                            for officer in active_officers:
                                dob = officer.get("date_of_birth", {})
                                officer["birth_year"] = dob.get("year", 9999)
                            
                            # Sort ascending to pull the highest numerical birth year (youngest)
                            active_officers.sort(key=lambda x: x["birth_year"], reverse=True)
                            target_director = active_officers[0]
                            
                            # Map full legal string tokens to individual variables
                            raw_name = target_director.get("name", "")
                            if "," in raw_name:
                                parts = raw_name.split(",")
                                director_last = parts[0].strip()
                                director_first = parts[1].strip().split(" ")[0]
                            else:
                                parts = raw_name.split(" ")
                                director_first = parts[0].strip()
                                director_last = parts[-1].strip()

                # -------------------------------------------------------------
                # STAGE 3: Hunter.io API Profile Email Verification
                # -------------------------------------------------------------
                # Programmatic gate: Only consume lookup if domain and name were resolved
                if domain != "N/A" and director_first != "N/A":
                    hunter_url = f"https://api.hunter.io/v2/email-finder?domain={domain}&first_name={director_first}&last_name={director_last}&api_key={hunter_key}"
                    hunter_res = await client.get(hunter_url)
                    
                    if hunter_res.status_code == 200:
                        hunter_data = hunter_res.json().get("data", {})
                        confidence = hunter_data.get("score", 0)
                        
                        # Strict data quality standard gate: Reject entries scoring under 90%
                        if confidence >= 90:
                            email_address = hunter_data.get("email", "N/A")

                # -------------------------------------------------------------
                # STAGE 4: FullEnrich API Direct Premium Mobile Lookup
                # -------------------------------------------------------------
                # Dual verification: Must have passed previous filters AND remain within slider volume cap
                if email_address != "N/A" and mobile_lookup_counter < mobile_cap:
                    fe_payload = {
                        "first_name": director_first,
                        "last_name": director_last,
                        "company_name": company_name,
                        "domain": domain
                    }
                    fe_headers = {
                        "Authorization": f"Bearer {fullenrich_key}",
                        "Content-Type": "application/json"
                    }
                    fe_url = "https://api.fullenrich.com/v1/enrich" # Example destination endpoint
                    
                    fe_res = await client.post(fe_url, json=fe_payload, headers=fe_headers)
                    if fe_res.status_code == 200:
                        fe_data = fe_res.json()
                        phone_number = fe_data.get("phone", "N/A")
                        linkedin_url = fe_data.get("linkedin", "N/A")
                        # Increment threshold allocation counters upon successful execution
                        mobile_lookup_counter += 1

            except Exception as e:
                # Standard fail safe fallback logging to protect full script looping failures
                print(f"Error executing pipeline sequence on index {index}: {str(e)}")

            # Append the structured dataset layer output object mapping back to Land Registry values
            processed_leads.append({
                "Title number": row.get("Title number", ""),
                "Property Address": row.get("Property Address", ""),
                "Post code": row.get("Post code", ""),
                "Price paid": row.get("Price paid", ""),
                "Proprietor name": company_name,
                "Company registered number": company_number,
                "Date proprietor added": row.get("Date proprietor added", ""),
                "Director first name": director_first,
                "Last name": director_last,
                "Domain name": domain,
                "Email address": email_address,
                "Linked in": linkedin_url,
                "Director phone number": phone_number
            })

            # Real-time reactive dashboard tracker updating active loop counts
            jobs[job_id]["progress"] = index + 1
            await asyncio.sleep(0.1) # Prevents thread exhaustion locally

    # Persist complete data array inside runtime memory state for download handling
    jobs[job_id]["status"] = "completed"
    jobs[job_id]["results"] = processed_leads

@app.post("/api/scrape/start")
async def start_enrichment_task(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    ch_key: str = Form(...),
    google_key: str = Form(...),
    google_cx: str = Form(...),
    hunter_key: str = Form(...),
    fullenrich_key: str = Form(...),
    mobile_cap: int = Form(...) # The dynamic volume valve value submitted via Next.js interface slider
):
    """
    Ingestion endpoint initializing background tasks without stalling API traffic loops.
    """
    file_bytes = await file.read()
    # Read client file into a raw programmatic frame using Pandas
    df = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
    
    # Generate simple time tracking string to index background states uniquely
    job_id = f"job_{int(asyncio.get_event_loop().time())}"
    jobs[job_id] = {"status": "pending", "progress": 0, "total": len(df), "results": []}
    
    # Hand off processing to asynchronous core background thread pool
    background_tasks.add_task(
        execute_enrichment_pipeline,
        job_id, df, ch_key, google_key, google_cx, hunter_key, fullenrich_key, mobile_cap
    )
    
    return {"job_id": job_id, "status": "processing initiated"}

@app.get("/api/scrape/status/{job_id}")
async def fetch_enrichment_status(job_id: str):
    """
    Polled tracking endpoint used to drive progress state visualizations on the dashboard UI.
    """
    job_state = jobs.get(job_id, {"status": "not_found"})
    # Strip comprehensive data results footprint out of standard verification loops to keep footprints light
    return {
        "status": job_state.get("status"),
        "progress": job_state.get("progress"),
        "total": job_state.get("total")
    }

@app.get("/api/scrape/download/{job_id}")
async def download_enriched_dataset(job_id: str):
    """
    Retrieval route called exclusively upon pipeline task reaching completed state status.
    """
    job_state = jobs.get(job_id)
    if not job_state or job_state["status"] != "completed":
        return {"error": "Dataset retrieval unavailable or active task pending execution"}
    
    # Parse cached profile results back into download document outputs
    export_df = pd.DataFrame(job_state["results"])
    stream = io.StringIO()
    export_df.to_csv(stream, index=False)
    
    # Return formatted text streaming directly down to frontend client system download environments
    return {
        "filename": f"enriched_leads_{job_id}.csv",
        "data": stream.getvalue()
    }
