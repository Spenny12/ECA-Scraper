// Mirrors the Pydantic models in models.py (backend, read-only reference —
// do not edit the Python side from here).

export type PipelineStatus = "Processing" | "Complete" | "Error";

export interface PipelineJob {
  id: string;
  status: PipelineStatus;
  created_at: number;
  updated_at: number;
  total_companies_in: number;
  domains_discovered_count: number;
  total_emails_found_count: number;
  final_combined_results: ResultRow[];
}

// One row per contact, shaped by pipeline.py's combine_results()/_rows_for_company().
export interface ResultRow {
  "Target Company Name": string;
  "Property Address": string;
  "Purchase Year": string;
  "Companies House Number": string;
  "Registered Address": string;
  "Discovered Domain": string;
  "Employee Name": string;
  "Employee Position": string;
  "Employee Email": string;
  Status: string;
}
