"""Pydantic models shared across the pipeline, integrations, and API layer."""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class DomainInfo(BaseModel):
    provided_name: str
    discovered_domain: Optional[str] = None
    discovery_method: Optional[str] = None
    search_query_used: Optional[str] = None


class EmployeeEmail(BaseModel):
    email: str
    type: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    position: Optional[str] = None


class ContactDiscoveryResult(BaseModel):
    domain: str
    emails_found: List[EmployeeEmail] = []


class CHCompanyData(BaseModel):
    company_name: str
    company_number: Optional[str] = None
    registered_address: Optional[str] = None
    directors: List[Dict[str, str]] = []


class PipelineJob(BaseModel):
    id: str
    status: str
    created_at: float
    updated_at: float
    total_companies_in: int = 0
    domains_discovered_count: int = 0
    total_emails_found_count: int = 0
    final_combined_results: List[Dict[str, Any]] = []


class BulkEmailJob(BaseModel):
    id: str
    status: str
    created_at: float
    updated_at: float
    total_contacts_uploaded: int = 0
    emails_sent_count: int = 0
    emails_skipped_count: int = 0
    errors: List[str] = []
