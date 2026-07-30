import { useState } from "react";
import { downloadPipelineResults } from "../lib/api";

interface ExportButtonProps {
  jobId: string;
}

export default function ExportButton({ jobId }: ExportButtonProps) {
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleClick() {
    setDownloading(true);
    setError(null);
    try {
      await downloadPipelineResults(jobId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Export failed.");
    } finally {
      setDownloading(false);
    }
  }

  return (
    <div>
      <button
        onClick={handleClick}
        disabled={downloading}
        className="w-full rounded-lg bg-brand-teal px-4 py-2.5 text-sm font-bold text-white transition-colors hover:bg-brand-teal-dark disabled:cursor-not-allowed disabled:opacity-50"
      >
        {downloading ? "Preparing CSV…" : "Export results (.csv)"}
      </button>
      {error && <p className="mt-2 text-sm text-red-600">{error}</p>}
    </div>
  );
}
