import { useState } from "react";
import ecaLogo from "./assets/eca-logo.png";
import ConfigPanel, { type ConfigPanelSubmitValues } from "./components/ConfigPanel";
import MonitoringWindow from "./components/MonitoringWindow";
import { uploadLeads } from "./lib/api";

// Persisted per-tab so an accidental refresh mid-run doesn't strand the
// operator with no way back to a job that's already spending API credits.
const JOB_ID_STORAGE_KEY = "eca-active-job-id";

export default function App() {
  const [jobId, setJobId] = useState<string | null>(() => sessionStorage.getItem(JOB_ID_STORAGE_KEY));
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  function startJob(id: string) {
    sessionStorage.setItem(JOB_ID_STORAGE_KEY, id);
    setJobId(id);
  }

  function clearJob() {
    sessionStorage.removeItem(JOB_ID_STORAGE_KEY);
    setJobId(null);
  }

  async function handleSubmit(values: ConfigPanelSubmitValues) {
    setSubmitting(true);
    setSubmitError(null);
    try {
      const job = await uploadLeads({
        file: values.file,
        hunterApiKey: values.hunterApiKey,
        maxContactsPerDomain: values.maxContactsPerDomain,
      });
      startJob(job.id);
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : "Failed to start the pipeline.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-2xl items-center gap-3 px-4 py-4">
          <img src={ecaLogo} alt="ECA Tax Associates" className="h-9 w-auto" />
          <div className="h-6 w-px bg-slate-200" />
          <span className="text-sm font-semibold tracking-wide text-slate-500 uppercase">
            Lead Scraper
          </span>
        </div>
      </header>

      <div className="mx-auto max-w-2xl px-4 py-10">
        <div className="mb-6">
          <h1 className="text-2xl font-extrabold tracking-tight text-slate-900">
            Find your next <span className="text-brand-coral">contact</span>
          </h1>
          <p className="mt-1 text-sm text-slate-500">
            Upload a Companies House / Land Registry export to discover company domains and director
            contact emails.
          </p>
        </div>

        <div className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
          {jobId ? (
            <MonitoringWindow jobId={jobId} onStartNewRun={clearJob} />
          ) : (
            <>
              <ConfigPanel onSubmit={handleSubmit} submitting={submitting} />
              {submitError && (
                <p className="mt-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
                  {submitError}
                </p>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
