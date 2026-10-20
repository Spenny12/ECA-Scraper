import { useEffect, useState } from "react";
import { getPipelineStatus, JobNotFoundError } from "../lib/api";
import type { PipelineJob } from "../types";
import ExportButton from "./ExportButton";

const POLL_INTERVAL_MS = 1500;

type DisplayStatus = "Pending" | "Processing" | "Completed" | "Failed" | "Expired";

interface MonitoringWindowProps {
  jobId: string;
  onStartNewRun: () => void;
}

function toDisplayStatus(job: PipelineJob | null, expired: boolean): DisplayStatus {
  if (expired) return "Expired";
  if (!job) return "Pending";
  if (job.status === "Complete") return "Completed";
  if (job.status === "Error") return "Failed";
  return "Processing";
}

const STATUS_STYLES: Record<DisplayStatus, string> = {
  Pending: "bg-slate-100 text-slate-600",
  Processing: "bg-brand-coral/10 text-brand-coral border border-brand-coral/30",
  Completed: "bg-brand-teal/10 text-brand-teal border border-brand-teal/30",
  Failed: "bg-red-50 text-red-700 border border-red-200",
  Expired: "bg-slate-100 text-slate-500",
};

export default function MonitoringWindow({ jobId, onStartNewRun }: MonitoringWindowProps) {
  const [job, setJob] = useState<PipelineJob | null>(null);
  const [expired, setExpired] = useState(false);
  const [pollError, setPollError] = useState<string | null>(null);

  useEffect(() => {
    // Self-scheduling: the next poll is only queued once the previous one
    // resolves, so a slow response (e.g. a Render cold start) can never
    // race a later one and overwrite a terminal status with a stale
    // "Processing" snapshot.
    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    async function poll() {
      try {
        const latest = await getPipelineStatus(jobId);
        if (cancelled) return;
        setJob(latest);
        setPollError(null);
        if (latest.status === "Processing") {
          timeoutId = setTimeout(poll, POLL_INTERVAL_MS);
        }
      } catch (err) {
        if (cancelled) return;
        if (err instanceof JobNotFoundError) {
          setExpired(true);
          return;
        }
        setPollError(err instanceof Error ? err.message : "Failed to check job status.");
        timeoutId = setTimeout(poll, POLL_INTERVAL_MS);
      }
    }

    poll();

    return () => {
      cancelled = true;
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, [jobId]);

  const displayStatus = toDisplayStatus(job, expired);
  const isProcessing = displayStatus === "Processing" || displayStatus === "Pending";

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs text-slate-400">Job ID</p>
          <p className="font-mono text-sm text-slate-600">{jobId}</p>
        </div>
        <span className={`rounded-full px-3 py-1 text-xs font-semibold ${STATUS_STYLES[displayStatus]}`}>
          {displayStatus}
        </span>
      </div>

      <div className="h-2 w-full overflow-hidden rounded-full bg-slate-100">
        <div
          className={`h-full rounded-full transition-all duration-500 ${
            displayStatus === "Failed" || displayStatus === "Expired"
              ? "w-full bg-red-500"
              : displayStatus === "Completed"
                ? "w-full bg-brand-teal"
                : "w-1/3 animate-pulse bg-brand-coral"
          }`}
        />
      </div>
      {isProcessing && (
        <p className="text-xs text-slate-400">
          Processing — this pipeline runs sequentially (domains → Companies House → emails), so counts below
          populate in stages rather than continuously. Checking every 1.5s.
        </p>
      )}

      <div className="grid grid-cols-3 gap-3">
        <StatBlock label="Companies in" value={job?.total_companies_in ?? "—"} />
        <StatBlock label="Domains found" value={job?.domains_discovered_count ?? "—"} />
        <StatBlock label="Emails found" value={job?.total_emails_found_count ?? "—"} />
      </div>

      {pollError && (
        <p className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          {pollError} (will keep retrying)
        </p>
      )}

      {displayStatus === "Expired" && (
        <p className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">
          This job is no longer available — jobs are held in memory for 24 hours only.
        </p>
      )}

      {displayStatus === "Failed" && (
        <p className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
          The pipeline hit an error while processing this file. This usually means the column headers
          didn't match the expected format, or the file has more than 6,000 companies (the per-run limit).
          Check the file and try again.
        </p>
      )}

      {displayStatus === "Completed" && <ExportButton jobId={jobId} />}

      {(displayStatus === "Completed" || displayStatus === "Failed" || displayStatus === "Expired") && (
        <button
          onClick={onStartNewRun}
          className="w-full rounded-lg border border-slate-300 px-4 py-2.5 text-sm font-medium text-slate-700 transition-colors hover:bg-slate-50"
        >
          Start new run
        </button>
      )}
    </div>
  );
}

function StatBlock({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-center">
      <p className="text-lg font-bold text-slate-900">{value}</p>
      <p className="text-xs text-slate-500">{label}</p>
    </div>
  );
}
