"""In-memory job storage and the Supabase-backed unsubscribe blacklist.

Kept in one place so every other module talks to jobs/the blacklist through
here rather than reaching into globals directly.
"""
import time
from typing import Dict, Optional

from supabase import Client, create_client

from config import JOB_TTL_SECONDS, SUPABASE_KEY, SUPABASE_URL, logger
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
