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
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://127.0.0.1:8000")

# --- Upload / job limits ---
# Hard caps to stop a bad/huge upload from exhausting memory or running up
# API bills against Serper / Companies House / Hunter.
MAX_UPLOAD_BYTES = 15 * 1024 * 1024        # 15 MB per uploaded file
MAX_COMPANIES_PER_UPLOAD = 3000            # rows fed into the enrichment pipeline
MAX_CONTACTS_PER_UPLOAD = 20000            # rows fed into the bulk sender

# In-memory jobs are never persisted, so they're purged after this long to
# stop process memory growing forever and to limit how long enriched PII
# sits in RAM.
JOB_TTL_SECONDS = 24 * 60 * 60             # 24 hours
