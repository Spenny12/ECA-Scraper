"""Pipeline job storage.

Jobs live in memory while they run. Finished pipeline jobs are also copied
to a private Supabase Storage bucket, because Render's free tier spins the
instance down ~15 minutes after the last request and takes memory with it -
get_pipeline_job() transparently reloads a job from there when it's gone.

Kept in one place so every other module talks to jobs through here rather
than reaching into globals directly.
"""
import json
import re
import threading
import time
from datetime import datetime
from typing import Dict, Optional

from supabase import Client, create_client

from config import JOB_TTL_SECONDS, SUPABASE_JOBS_BUCKET, SUPABASE_KEY, SUPABASE_URL, logger
from models import PipelineJob

pipeline_jobs: Dict[str, PipelineJob] = {}

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
else:
    logger.warning("Supabase credentials missing. Finished jobs will only be kept in memory.")


# Job IDs come straight from the URL, so only ever turn ones matching the
# formats main.py generates (ECA-0042, or older eca_job_3fafe377) into a
# storage path.
PIPELINE_JOB_ID_RE = re.compile(r'^(ECA-\d{4,}|eca_job_[0-9a-f]{8})$')
JOB_NUMBER_RE = re.compile(r'^ECA-(\d+)(\.json)?$')
COUNTER_PATH = "_job_counter.json"
_counter_lock = threading.Lock()
_last_job_number = 0


def next_job_id() -> str:
    """Returns the next sequential job ID (ECA-0001, ECA-0002, ...) - used
    to tell runs apart in HubSpot, so it must never repeat. The counter is
    kept in the Supabase bucket so it survives restarts, and is also checked
    against the highest job already in memory or the bucket in case the
    counter file is ever lost (blocking - call via asyncio.to_thread)."""
    global _last_job_number
    with _counter_lock:
        highest = max([_last_job_number] + [int(m.group(1)) for j in pipeline_jobs if (m := JOB_NUMBER_RE.match(j))])
        if supabase:
            bucket = supabase.storage.from_(SUPABASE_JOBS_BUCKET)
            try:
                highest = max(highest, int(json.loads(bucket.download(COUNTER_PATH))["last"]))
            except Exception:
                pass  # no counter yet - fall back to the jobs in the bucket
            try:
                highest = max([highest] + [int(m.group(1)) for item in bucket.list(options={"limit": 1000})
                                           if (m := JOB_NUMBER_RE.match(item.get("name", "")))])
            except Exception as e:
                logger.warning(f"Couldn't list Supabase jobs while numbering a new job: {e}")
        _last_job_number = highest + 1
        if supabase:
            try:
                supabase.storage.from_(SUPABASE_JOBS_BUCKET).upload(
                    COUNTER_PATH, json.dumps({"last": _last_job_number}).encode("utf-8"),
                    file_options={"content-type": "application/json", "upsert": "true"},
                )
            except Exception as e:
                logger.error(f"Failed to save the job counter to Supabase Storage: {e}")
        return f"ECA-{_last_job_number:04d}"


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
            # Only job files expire - the job counter must never be purged.
            if not created or not PIPELINE_JOB_ID_RE.match(item.get("name", "").removesuffix(".json")):
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

    Without this the in-memory dict grows forever for the
    life of the process and hold enriched contact data indefinitely.
    """
    cutoff = time.time() - JOB_TTL_SECONDS
    stale_ids = [jid for jid, job in pipeline_jobs.items() if job.updated_at < cutoff]
    for jid in stale_ids:
        del pipeline_jobs[jid]
    if stale_ids:
        logger.info(f"Purged {len(stale_ids)} stale job(s): {stale_ids}")
