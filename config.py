"""Environment-driven configuration, tunable limits, and the shared logger."""
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger("ECA_Pipeline")

# --- API keys / credentials (set on Render as environment variables) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SERPER_API_KEY = os.environ.get("SERPER_API_KEY")
CH_API_KEY = os.environ.get("CH_API_KEY")
HUBSPOT_ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN")
FULLENRICH_API_KEY = os.environ.get("FULLENRICH_API_KEY")
# Private Supabase Storage bucket that finished pipeline jobs are copied to,
# so they survive Render's free-tier spin-down (which wipes in-memory jobs
# ~15 minutes after the last request). Needs the service-role key above.
SUPABASE_JOBS_BUCKET = os.environ.get("SUPABASE_JOBS_BUCKET", "pipeline-jobs")

# --- Upload / job limits ---
# Hard caps to stop a bad/huge upload from exhausting memory or running up
# API bills against Serper / Companies House / Hunter.
MAX_UPLOAD_BYTES = 15 * 1024 * 1024        # 15 MB per uploaded file
MAX_COMPANIES_PER_UPLOAD = 3000            # rows fed into the enrichment pipeline
# Only contacts whose domain match confidence is strictly above this are
# sent to FullEnrich, which charges per result.
FULLENRICH_MIN_CONFIDENCE = 0.8

# Jobs are purged after this long - from memory, and finished pipeline jobs
# from the Supabase bucket too - to stop memory growing forever and to limit
# how long enriched PII is kept.
JOB_TTL_SECONDS = 24 * 60 * 60             # 24 hours
