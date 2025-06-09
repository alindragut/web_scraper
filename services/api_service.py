import logging
from typing import Optional, List

from src.utils.logging_setup import setup_app_logging
setup_app_logging("APIService")

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.utils import config
from src.utils import normalization_utils
from src.utils import elastic_search_utils as es_utils

class InputCompany(BaseModel):
    """Defines the structure of the company data sent to the API."""
    name: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    facebook: Optional[str] = Field(None, alias="input_facebook")

class CompanyProfileResponse(BaseModel):
    """Defines the structure of the company profile returned by the API."""
    url: Optional[str] = None
    company_name: Optional[str] = None
    phone_numbers: List[str] = []
    social_media_links: List[str] = []
    addresses: List[str] = []

app = FastAPI(
    title="Company Matcher API",
    description="An API to find the best matching company profile from a database.",
    version="1.0.0"
)

# Initialize logger for the API service
log = logging.getLogger("APIService")


@app.on_event("startup")
async def startup_event():
    """On startup, get the ES client to ensure a connection is available."""
    es_utils.get_es_client()
    log.info("API service started and connected to Elasticsearch.")

@app.post("/match", response_model=CompanyProfileResponse)
async def match_company(company_input: InputCompany):
    """
    Accepts company details and returns the single best matching company profile from Elasticsearch.
    """
    log.info(f"Received match request: {company_input.dict()}")
    
    # Normalize the input using the exact same utility functions as in the storage service.
    norm_name = normalization_utils.normalize_company_name(company_input.name) if company_input.name else None
    norm_phone = normalization_utils.normalize_phone_number(company_input.phone) if company_input.phone else None
    norm_domain = normalization_utils.get_domain_from_url(company_input.website) if company_input.website else None
    norm_facebook = normalization_utils.normalize_social_media_profile(company_input.facebook) if company_input.facebook else None

    query_parts = []
    
    # Phone match is the strongest signal of a match.
    if norm_phone:
        query_parts.append({"term": {"normalized_phone_numbers": {"value": norm_phone, "boost": 10.0}}})
    
    # Domain is also a very strong signal.
    if norm_domain:
        query_parts.append({"term": {"domain": {"value": norm_domain, "boost": 8.0}}})
        
    # Facebook profile is a good signal.
    if norm_facebook:
        query_parts.append({"term": {"social_media_profiles": {"value": norm_facebook, "boost": 5.0}}})
        
    # Company name is a good but weaker signal.
    if norm_name:
        query_parts.append({"match": {"searchable_name": {"query": norm_name, "boost": 2.0}}})
        
    if not query_parts:
        raise HTTPException(status_code=400, detail="At least one input field (name, phone, website, or facebook) must be provided.")

    es_query = {
        "query": {
            "bool": {
                "should": query_parts,
                "minimum_should_match": 1  # At least one condition must match
            }
        }
    }
    
    log.info(f"Executing Elasticsearch query: {es_query}")
    es_client = es_utils.get_es_client()
    try:
        response = es_client.search(
            index=config.ELASTICSEARCH_INDEX_NAME,
            body=es_query,
            size=1  # We only want the top-scoring result.
        )
    except Exception as e:
        log.error(f"Elasticsearch query failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while searching for company profiles.")
        
    hits = response['hits']['hits']
    if not hits:
        log.warning(f"No match found for input: {company_input.dict()}")
        raise HTTPException(status_code=404, detail="No matching company profile found.")
        
    best_match = hits[0]['_source']
    log.info(f"Found best match with score {hits[0]['_score']}: {best_match.get('company_name')}")
    
    return best_match

if __name__ == "__main__":
    # This allows running the API directly for local testing without Docker.
    uvicorn.run("services.api_service:app", host="0.0.0.0", port=8000, reload=True)