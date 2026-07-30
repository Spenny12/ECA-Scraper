import type { PipelineJob } from "../types";

const RAW_API_BASE = import.meta.env.VITE_API_BASE_URL;

if (!RAW_API_BASE) {
  throw new Error(
    "VITE_API_BASE_URL is not set. Configure it as a build environment variable " +
      "(e.g. in Cloudflare Pages project settings) for both Production and Preview."
  );
}

// Strip any trailing slash(es) so a value like "https://host.com/" doesn't
// produce a double slash once this module appends its own leading "/path".
const API_BASE = RAW_API_BASE.replace(/\/+$/, "");

const STATUS_TIMEOUT_MS = 30_000;
const UPLOAD_TIMEOUT_MS = 60_000;

function withTimeout(ms: number) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  return { signal: controller.signal, cancel: () => clearTimeout(id) };
}

async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  const { signal, cancel } = withTimeout(timeoutMs);
  try {
    return await fetch(url, { ...init, signal });
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") {
      throw new Error("Request timed out. Check your connection and try again.");
    }
    throw err;
  } finally {
    cancel();
  }
}

async function parseErrorDetail(res: Response): Promise<string> {
  try {
    const body = await res.json();
    if (typeof body?.detail === "string") return body.detail;
  } catch {
    // response wasn't JSON - fall through to generic message
  }
  return `Request failed (HTTP ${res.status})`;
}

export interface UploadLeadsOptions {
  file: File;
  hunterApiKey?: string;
  maxContactsPerDomain: number;
}

export async function uploadLeads({
  file,
  hunterApiKey,
  maxContactsPerDomain,
}: UploadLeadsOptions): Promise<PipelineJob> {
  const form = new FormData();
  form.append("file", file);
  if (hunterApiKey) form.append("hunter_api_key", hunterApiKey);
  form.append("max_contacts_per_domain", String(maxContactsPerDomain));

  const res = await fetchWithTimeout(`${API_BASE}/upload-leads/`, { method: "POST", body: form }, UPLOAD_TIMEOUT_MS);
  if (!res.ok) throw new Error(await parseErrorDetail(res));
  return res.json();
}

export class JobNotFoundError extends Error {}

export async function getPipelineStatus(jobId: string): Promise<PipelineJob> {
  const res = await fetchWithTimeout(
    `${API_BASE}/pipeline-status/${encodeURIComponent(jobId)}`,
    {},
    STATUS_TIMEOUT_MS
  );
  if (res.status === 404) throw new JobNotFoundError("Job not found or has expired.");
  if (!res.ok) throw new Error(await parseErrorDetail(res));
  return res.json();
}

export async function downloadPipelineResults(jobId: string): Promise<void> {
  const res = await fetchWithTimeout(
    `${API_BASE}/pipeline-results/${encodeURIComponent(jobId)}/export`,
    {},
    UPLOAD_TIMEOUT_MS
  );
  if (!res.ok) throw new Error(await parseErrorDetail(res));

  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${jobId}_leads.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  // Deferred: revoking immediately can abort an in-progress download in Safari.
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
