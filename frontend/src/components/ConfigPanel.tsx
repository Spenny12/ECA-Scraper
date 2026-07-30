import { useState, type DragEvent, type FormEvent } from "react";

const ALLOWED_EXTENSIONS = [".csv", ".xlsx", ".xls"];
const MAX_UPLOAD_MB = 15; // mirrors config.py MAX_UPLOAD_BYTES
const MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024;
const MAX_COMPANIES = 3000; // mirrors config.py MAX_COMPANIES_PER_UPLOAD

export interface ConfigPanelSubmitValues {
  file: File;
  hunterApiKey: string;
  maxContactsPerDomain: number;
}

interface ConfigPanelProps {
  onSubmit: (values: ConfigPanelSubmitValues) => void;
  submitting: boolean;
}

function validateFile(file: File): string | null {
  const lower = file.name.toLowerCase();
  if (!ALLOWED_EXTENSIONS.some((ext) => lower.endsWith(ext))) {
    return `Unsupported file type. Use one of: ${ALLOWED_EXTENSIONS.join(", ")}`;
  }
  if (file.size > MAX_UPLOAD_BYTES) {
    return `File too large (${(file.size / 1_000_000).toFixed(1)} MB). Max is ${MAX_UPLOAD_MB} MB.`;
  }
  return null;
}

export default function ConfigPanel({ onSubmit, submitting }: ConfigPanelProps) {
  const [file, setFile] = useState<File | null>(null);
  const [fileError, setFileError] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [hunterApiKey, setHunterApiKey] = useState("");
  const [maxContactsPerDomain, setMaxContactsPerDomain] = useState(10);

  function pickFile(candidate: File) {
    const error = validateFile(candidate);
    setFileError(error);
    setFile(error ? null : candidate);
  }

  function handleDrop(e: DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragActive(false);
    const dropped = e.dataTransfer.files?.[0];
    if (dropped) pickFile(dropped);
  }

  function handleSubmit(e: FormEvent) {
    e.preventDefault();
    if (!file) {
      setFileError("Choose a CSV or Excel export to upload.");
      return;
    }
    // Guards against transient invalid states while editing the number field
    // (e.g. briefly empty/NaN) reaching the API as an invalid value.
    const safeMaxContacts =
      Number.isFinite(maxContactsPerDomain) && maxContactsPerDomain > 0
        ? Math.min(50, Math.floor(maxContactsPerDomain))
        : 10;
    onSubmit({ file, hunterApiKey: hunterApiKey.trim(), maxContactsPerDomain: safeMaxContacts });
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-6">
      <div>
        <label className="mb-2 block text-sm font-medium text-slate-700">
          Companies House / Land Registry export
        </label>
        <div
          onDragOver={(e) => {
            e.preventDefault();
            setDragActive(true);
          }}
          onDragLeave={() => setDragActive(false)}
          onDrop={handleDrop}
          className={`flex flex-col items-center justify-center rounded-lg border-2 border-dashed px-6 py-10 text-center transition-colors ${
            dragActive ? "border-brand-teal bg-brand-teal/5" : "border-slate-300 bg-slate-50"
          }`}
        >
          <input
            id="file-input"
            type="file"
            accept={ALLOWED_EXTENSIONS.join(",")}
            className="sr-only"
            onChange={(e) => {
              const picked = e.target.files?.[0];
              if (picked) pickFile(picked);
            }}
          />
          <label htmlFor="file-input" className="cursor-pointer text-sm text-slate-600">
            {file ? (
              <span className="font-semibold text-brand-teal">{file.name}</span>
            ) : (
              <>
                <span className="font-semibold text-brand-teal">Click to browse</span> or drag a file here
                <div className="mt-1 text-xs text-slate-400">
                  CSV, XLSX, or XLS — up to {MAX_UPLOAD_MB} MB, {MAX_COMPANIES.toLocaleString()} companies per run
                </div>
              </>
            )}
          </label>
        </div>
        {fileError && <p className="mt-2 text-sm text-red-600">{fileError}</p>}
      </div>

      <div>
        <label htmlFor="hunter-key" className="mb-2 block text-sm font-medium text-slate-700">
          Hunter.io API key <span className="text-slate-400">(optional — enables email discovery)</span>
        </label>
        <input
          id="hunter-key"
          type="password"
          autoComplete="off"
          value={hunterApiKey}
          onChange={(e) => setHunterApiKey(e.target.value)}
          placeholder="Leave blank to skip email discovery"
          className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400 focus:border-brand-teal focus:ring-1 focus:ring-brand-teal focus:outline-none"
        />
      </div>

      <div>
        <label htmlFor="max-contacts" className="mb-2 block text-sm font-medium text-slate-700">
          Max contacts per domain
        </label>
        <input
          id="max-contacts"
          type="number"
          min={1}
          max={50}
          value={maxContactsPerDomain}
          onChange={(e) => setMaxContactsPerDomain(Number(e.target.value))}
          className="w-32 rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 focus:border-brand-teal focus:ring-1 focus:ring-brand-teal focus:outline-none"
        />
      </div>

      <button
        type="submit"
        disabled={submitting}
        className="w-full rounded-lg bg-brand-teal px-4 py-2.5 text-sm font-bold text-white transition-colors hover:bg-brand-teal-dark disabled:cursor-not-allowed disabled:opacity-50"
      >
        {submitting ? "Starting…" : "Start Scraper"}
      </button>
    </form>
  );
}
