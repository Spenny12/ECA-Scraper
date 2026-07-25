"""Tool 2: the bulk email sender."""
import asyncio
import io
import re
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from jinja2.sandbox import SandboxedEnvironment

import store
from config import MAX_CONTACTS_PER_UPLOAD, RENDER_EXTERNAL_URL, logger

SMTP_HOST = "smtp.office365.com"
SMTP_PORT = 587
SKIP_MARKERS = ("none found", "nan")


async def execute_bulk_send(job_id, file_bytes, filename, smtp_user, smtp_pass, email_subject, email_body):
    job = store.email_jobs[job_id]
    logger.info(f"Starting Bulk Send for Job {job_id}")

    try:
        df = _read_contacts(file_bytes, filename)
        job.total_contacts_uploaded = len(df)

        if len(df) > MAX_CONTACTS_PER_UPLOAD:
            raise ValueError(
                f"Upload contains {len(df)} rows, which exceeds the {MAX_CONTACTS_PER_UPLOAD} "
                f"limit per send. Split the file and re-upload."
            )

        blacklist = store.load_blacklist()
        template = _build_template(email_body)

        # smtplib is fully synchronous/blocking. Running the send loop
        # directly in this async function would freeze the whole event loop
        # - and every other request/job on the server - for the entire send.
        await asyncio.to_thread(_send_all, job, df, blacklist, template, smtp_user, smtp_pass, email_subject)

    except Exception as e:
        logger.error(f"Bulk send error {job_id}: {e}")
        job.status = "Error"
        job.errors.append(str(e))
    finally:
        if job.status != "Error":
            job.status = "Complete"
        job.updated_at = time.time()
        logger.info(f"Bulk Send {job_id} complete. Sent {job.emails_sent_count}.")


def _read_contacts(file_bytes: bytes, filename: str) -> pd.DataFrame:
    if filename.lower().endswith('.csv'):
        try:
            return pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
        except UnicodeDecodeError:
            return pd.read_csv(io.StringIO(file_bytes.decode('iso-8859-1')))
    return pd.read_excel(io.BytesIO(file_bytes))


def _build_template(email_body: str):
    # Convert {variables} to Jinja2 {{ variables }} format
    converted_body = re.sub(r'(?<!\{)\{([a-zA-Z0-9_]+)\}(?!\})', r'{{\1}}', email_body)
    template_str = f"""
    {converted_body}
    <br><br>
    <p style="font-size: 10px; color: gray;">
        If you no longer wish to receive emails from me, <a href="{RENDER_EXTERNAL_URL}/unsubscribe?email={{{{ email }}}}">click here to unsubscribe</a>.
    </p>
    """.replace('\n', '<br>')
    # SandboxedEnvironment blocks attribute/dunder access that would
    # otherwise let a crafted email_body execute arbitrary Python
    # (server-side template injection) on the server.
    return SandboxedEnvironment(autoescape=False).from_string(template_str)


def _send_all(job, df, blacklist, template, smtp_user, smtp_pass, email_subject):
    """Blocking SMTP send loop - runs in a worker thread via asyncio.to_thread."""
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    try:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        for _, row in df.iterrows():
            _send_one(server, job, row, blacklist, template, smtp_user, email_subject)
    finally:
        try:
            server.quit()
        except Exception:
            pass


def _send_one(server, job, row, blacklist, template, smtp_user, email_subject):
    target_email = str(row.get("Employee Email", "")).strip().lower()
    if not target_email or target_email in blacklist or any(m in target_email for m in SKIP_MARKERS):
        job.emails_skipped_count += 1
        return

    company_name = str(row.get("Target Company Name", "your company")).strip()
    employee_name = str(row.get("Employee Name", "there")).strip()
    prop_addr = _clean(row.get("Property Address"), "your property")
    purch_year = _clean(row.get("Purchase Year"), "recently")
    first_name = employee_name.split()[0] if employee_name and employee_name != "UNIDENTIFIED" else "there"

    html_body = template.render(
        director_first_name=first_name, company=company_name,
        property_address=prop_addr, purchase_year=purch_year, email=target_email,
    )

    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = target_email
    msg['Subject'] = email_subject
    msg.attach(MIMEText(html_body, 'html'))

    try:
        server.send_message(msg)
        job.emails_sent_count += 1
        time.sleep(2)  # blocking sleep is fine here - we're already off the event loop
    except Exception as e:
        job.errors.append(f"Failed to send to {target_email}: {str(e)}")
        logger.warning(f"Send failed for {target_email}: {e}")


def _clean(value, default: str) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text and text != "nan" else default
