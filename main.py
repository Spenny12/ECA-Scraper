"""ECA Pipeline API - routes and wiring only.

/upload-leads/ and /pipeline-status/ enrich a company/land-registry sheet
with domains, Companies House data, and director emails; the results are
exported as CSV (and imported into HubSpot, where outreach is handled).

The actual logic lives in pipeline.py; see store.py for job storage and
config.py for env/limits.
"""
import asyncio
import time
import uuid
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

import store
from config import CH_API_KEY, MAX_UPLOAD_BYTES, SERPER_API_KEY, logger
from hubspot_sync import HubSpotError, push_results_to_hubspot
from models import PipelineJob
from pipeline import execute_full_pipeline, results_to_csv_bytes

app = FastAPI(title="ECA Pipeline API", description="Lead Enrichment")

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
    await asyncio.to_thread(store.ensure_jobs_bucket)

    async def loop():
        while True:
            # Runs once at startup too - on Render's free tier the instance
            # rarely stays up a full hour, so persisted jobs would otherwise
            # never get purged.
            store.purge_stale_jobs()
            await asyncio.to_thread(store.purge_persisted_jobs)
            await asyncio.sleep(60 * 60)  # hourly
    asyncio.create_task(loop())


async def _get_pipeline_job_or_404(job_id: str) -> PipelineJob:
    job = await asyncio.to_thread(store.get_pipeline_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


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


@app.post("/upload-leads/", response_model=PipelineJob)
async def upload_leads_csv_start_pipeline(
    file: UploadFile = File(...),
    hunter_api_key: Optional[str] = Form(None),
    # This caps how many Companies House directors per company get a Hunter
    # Email Finder lookup attempted (1 Hunter credit per director, whether
    # or not an email is found) - it is NOT a guaranteed number of emails
    # returned. See hunter.py's discover_director_emails for details.
    max_directors_per_company: Optional[int] = Form(None),
    # The frontend sends the same setting under this name - accepted as an
    # alias so its "Max contacts per domain" field actually takes effect.
    max_contacts_per_domain: Optional[int] = Form(None),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    file_bytes = await file.read()
    _validate_upload(file, file_bytes)
    max_directors = max_directors_per_company or max_contacts_per_domain or 4

    job_id = f"eca_job_{uuid.uuid4().hex[:8]}"
    new_job = PipelineJob(id=job_id, status="Processing", created_at=time.time(), updated_at=time.time())
    store.pipeline_jobs[job_id] = new_job
    store.purge_stale_jobs()

    background_tasks.add_task(
        execute_full_pipeline, job_id, file_bytes, file.filename,
        SERPER_API_KEY, CH_API_KEY, hunter_api_key, max_directors
    )
    return new_job


@app.get("/pipeline-status/{job_id}", response_model=PipelineJob)
async def get_pipeline_job_status(job_id: str):
    return await _get_pipeline_job_or_404(job_id)


@app.get("/pipeline-results/{job_id}/export")
async def export_pipeline_results_csv(job_id: str):
    job = await _get_pipeline_job_or_404(job_id)
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
async def sync_pipeline_results_to_hubspot(job_id: str, dry_run: bool = False):
    """Pushes a job's results to HubSpot (create-only, see hubspot_sync.py).
    Safe to re-run - records already in HubSpot are skipped. With
    ?dry_run=true it only reports what would be created."""
    job = await _get_pipeline_job_or_404(job_id)
    if job.status != "Complete":
        raise HTTPException(status_code=409, detail=f"Job is not complete yet (status: {job.status}).")
    if not job.final_combined_results:
        raise HTTPException(status_code=404, detail="Job completed with no results to sync.")

    try:
        return await push_results_to_hubspot(job.final_combined_results, dry_run=dry_run)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HubSpotError as e:
        raise HTTPException(status_code=502, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info")
