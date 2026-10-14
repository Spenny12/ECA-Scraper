"""Job storage and the Supabase-backed unsubscribe blacklist.

Jobs live in memory while they run. Finished pipeline jobs are also copied
to a private Supabase Storage bucket, because Render's free tier spins the
instance down ~15 minutes after the last request and takes memory with it -
get_pipeline_job() transparently reloads a job from there when it's gone.

Kept in one place so every other module talks to jobs/the blacklist through
here rather than reaching into globals directly.
"""
import re
import time
from datetime import datetime
from typing import Dict, Optional

from supabase import Client, create_client

from config import JOB_TTL_SECONDS, SUPABASE_JOBS_BUCKET, SUPABASE_KEY, SUPABASE_URL, logger
from models import BulkEmailJob, PipelineJob

pipeline_jobs: Dict[str, PipelineJob] = {}
email_jobs: Dict[str, BulkEmailJob] = {}

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


# Job IDs come straight from the URL, so only ever turn ones matching the
# format main.py generates into a storage path.
PIPELINE_JOB_ID_RE = re.compile(r'^eca_job_[0-9a-f]{8}$')


def _job_path(job_id: str) -> str:
    return f"{job_id}.json"


def ensure_jobs_bucket():
    """Creates the private jobs bucket if it doesn't exist yet."""
    if not supabase:
        return
    try:
        supabase.storage.get_bucket(SUPABASE_JOBS_BUCKET)
    except Exception:
        try:
            supabase.storage.create_bucket(SUPABASE_JOBS_BUCKET, options={"public": False})
            logger.info(f"Created Supabase Storage bucket '{SUPABASE_JOBS_BUCKET}'.")
        except Exception as e:
            logger.error(f"Could not create Supabase Storage bucket '{SUPABASE_JOBS_BUCKET}': {e}")


def persist_pipeline_job(job: PipelineJob):
    """Copies a finished pipeline job to Supabase Storage (blocking - call
    via asyncio.to_thread from async code)."""
    if not supabase:
        return
    try:
        supabase.storage.from_(SUPABASE_JOBS_BUCKET).upload(
            _job_path(job.id),
            job.model_dump_json().encode('utf-8'),
            file_options={"content-type": "application/json", "upsert": "true"},
        )
        logger.info(f"Persisted job {job.id} to Supabase Storage.")
    except Exception as e:
        logger.error(f"Failed to persist job {job.id} to Supabase Storage: {e}")


def get_pipeline_job(job_id: str) -> Optional[PipelineJob]:
    """Returns the job from memory, falling back to Supabase Storage if the
    instance has restarted since it finished (blocking - call via
    asyncio.to_thread from async code). Returns None if not found/expired."""
    job = pipeline_jobs.get(job_id)
    if job or not supabase or not PIPELINE_JOB_ID_RE.match(job_id):
        return job
    try:
        raw = supabase.storage.from_(SUPABASE_JOBS_BUCKET).download(_job_path(job_id))
        job = PipelineJob.model_validate_json(raw)
    except Exception as e:
        # A missing object raises too, so this is the normal "not found" path.
        logger.info(f"Job {job_id} not found in Supabase Storage: {e}")
        return None
    if job.updated_at < time.time() - JOB_TTL_SECONDS:
        return None  # past its TTL - purge_persisted_jobs() will delete it
    pipeline_jobs[job_id] = job
    logger.info(f"Rehydrated job {job_id} from Supabase Storage.")
    return job


def purge_persisted_jobs():
    """Deletes persisted jobs older than JOB_TTL_SECONDS from Supabase
    Storage (blocking - call via asyncio.to_thread from async code)."""
    if not supabase:
        return
    try:
        bucket = supabase.storage.from_(SUPABASE_JOBS_BUCKET)
        cutoff = time.time() - JOB_TTL_SECONDS
        stale = []
        for item in bucket.list(options={"limit": 1000}):
            created = item.get("created_at")
            if not created:
                continue
            created_ts = datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()
            if created_ts < cutoff:
                stale.append(item["name"])
        if stale:
            bucket.remove(stale)
            logger.info(f"Purged {len(stale)} persisted job(s) from Supabase Storage.")
    except Exception as e:
        logger.error(f"Failed to purge persisted jobs from Supabase Storage: {e}")


def purge_stale_jobs():
    """Drops jobs (and the PII/emails they hold) older than JOB_TTL_SECONDS.

    Jobs only live in memory, so without this the dicts grow forever for the
    life of the process and hold enriched contact data indefinitely.
    """
    cutoff = time.time() - JOB_TTL_SECONDS
    for job_dict in (pipeline_jobs, email_jobs):
        stale_ids = [jid for jid, job in job_dict.items() if job.updated_at < cutoff]
        for jid in stale_ids:
            del job_dict[jid]
        if stale_ids:
            logger.info(f"Purged {len(stale_ids)} stale job(s): {stale_ids}")
