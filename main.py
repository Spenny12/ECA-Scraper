"""ECA Pipeline & Sender API - routes and wiring only.

Tool 1 (/upload-leads/, /pipeline-status/): enriches a company/land-registry
    sheet with domains, Companies House data, and employee emails.
Tool 2 (/bulk-send/, /bulk-send-status/): sends a templated email to each
    contact from a previous enrichment run.

The actual logic lives in pipeline.py (Tool 1) and emailer.py (Tool 2);
see store.py for job/blacklist storage and config.py for env/limits.
"""
import asyncio
import html
import time
import uuid
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

import store
from config import CH_API_KEY, MAX_UPLOAD_BYTES, SERPER_API_KEY, logger
from emailer import execute_bulk_send
from hubspot_sync import sync_results_to_hubspot
from models import BulkEmailJob, PipelineJob
from pipeline import execute_full_pipeline, results_to_csv_bytes

app = FastAPI(title="ECA Pipeline & Sender API", description="Lead Enrichment & Bulk Email Sender")

# NOTE: this API has no authentication and CORS is wide open. That's an
# accepted tradeoff for this deployment - it only runs behind an
# HTTP-login-protected private subdomain, and upstream API usage is capped
# and billed to the client. Revisit if the deployment model ever changes.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _start_job_cleanup_loop():
    async def loop():
        while True:
            await asyncio.sleep(60 * 60)  # hourly
            store.purge_stale_jobs()
    asyncio.create_task(loop())


def _validate_upload(file: UploadFile, file_bytes: bytes):
    if not file.filename.lower().endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type.")
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({len(file_bytes) / 1_000_000:.1f} MB). Max is {MAX_UPLOAD_BYTES / 1_000_000:.0f} MB."
        )


@app.get("/")
async def root():
    return {"message": "ECA API is operational."}


@app.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe(email: str):
    if not email:
        return "Invalid request."
    store.add_to_blacklist(email)
    logger.info(f"Unsubscribed: {email}")
    # Escape before interpolating into HTML - `email` is attacker-controlled
    # query input.
    safe_email = html.escape(email)
    return f"""
    <html>
        <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
            <h2>Unsubscribed Successfully</h2>
            <p><b>{safe_email}</b> has been securely removed from our mailing list.</p>
        </body>
    </html>
    """


# --- TOOL 1: THE DATA ENRICHER ---
@app.post("/upload-leads/", response_model=PipelineJob)
async def upload_leads_csv_start_pipeline(
    file: UploadFile = File(...),
    hunter_api_key: Optional[str] = Form(None),
    # This caps how many Companies House directors per company get a Hunter
    # Email Finder lookup attempted (1 Hunter credit per director, whether
    # or not an email is found) - it is NOT a guaranteed number of emails
    # returned. See hunter.py's discover_director_emails for details.
    max_directors_per_company: int = Form(4),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    file_bytes = await file.read()
    _validate_upload(file, file_bytes)

    job_id = f"eca_job_{uuid.uuid4().hex[:8]}"
    new_job = PipelineJob(id=job_id, status="Processing", created_at=time.time(), updated_at=time.time())
    store.pipeline_jobs[job_id] = new_job
    store.purge_stale_jobs()

    background_tasks.add_task(
        execute_full_pipeline, job_id, file_bytes, file.filename,
        SERPER_API_KEY, CH_API_KEY, hunter_api_key, max_directors_per_company
    )
    return new_job


@app.get("/pipeline-status/{job_id}", response_model=PipelineJob)
async def get_pipeline_job_status(job_id: str):
    if job_id not in store.pipeline_jobs:
        raise HTTPException(status_code=404, detail="Job not found.")
    return store.pipeline_jobs[job_id]


@app.get("/pipeline-results/{job_id}/export")
async def export_pipeline_results_csv(job_id: str):
    if job_id not in store.pipeline_jobs:
        raise HTTPException(status_code=404, detail="Job not found.")
    job = store.pipeline_jobs[job_id]
    if job.status != "Complete":
        raise HTTPException(status_code=409, detail=f"Job is not complete yet (status: {job.status}).")
    if not job.final_combined_results:
        raise HTTPException(status_code=404, detail="Job completed with no results to export.")

    csv_bytes = results_to_csv_bytes(job.final_combined_results)
    return StreamingResponse(
        iter([csv_bytes]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{job_id}_leads.csv"'},
    )


@app.post("/pipeline-results/{job_id}/sync-hubspot")
async def sync_pipeline_results_to_hubspot(job_id: str):
    if job_id not in store.pipeline_jobs:
        raise HTTPException(status_code=404, detail="Job not found.")
    job = store.pipeline_jobs[job_id]
    if job.status != "Complete":
        raise HTTPException(status_code=409, detail=f"Job is not complete yet (status: {job.status}).")
    if not job.final_combined_results:
        raise HTTPException(status_code=404, detail="Job completed with no results to sync.")

    try:
        return await sync_results_to_hubspot(job.final_combined_results)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


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
    file_bytes = await file.read()
    _validate_upload(file, file_bytes)

    job_id = f"email_job_{uuid.uuid4().hex[:8]}"
    new_job = BulkEmailJob(id=job_id, status="Processing", created_at=time.time(), updated_at=time.time())
    store.email_jobs[job_id] = new_job
    store.purge_stale_jobs()

    background_tasks.add_task(
        execute_bulk_send, job_id, file_bytes, file.filename, smtp_user, smtp_pass, email_subject, email_body
    )
    return new_job


@app.get("/bulk-send-status/{job_id}", response_model=BulkEmailJob)
async def get_bulk_send_status(job_id: str):
    if job_id not in store.email_jobs:
        raise HTTPException(status_code=404, detail="Email Job not found.")
    return store.email_jobs[job_id]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info")
