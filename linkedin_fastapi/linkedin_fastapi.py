import requests
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Union
import logging
import uuid
import time
import threading
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (restrict in production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for the request/response
class CampaignRequest(BaseModel):
    campaign_ids: List[str]

class LeadResponse(BaseModel):
    Name: str
    LinkedIn: str
    InputField: str

class JobRequest(BaseModel):
    campaign_ids: List[str]

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str

class JobStatus(BaseModel):
    job_id: str
    status: str
    message: str
    progress: Dict[str, Any]
    results: Optional[List[LeadResponse]]
    total_leads_processed: int
    total_leads_found: int
    processing_time: float
    created_at: str
    last_updated: str

# Global variables to store CSV data and jobs
apollo_df = None
personalized_messages_df = None

# Dictionary to store job information
jobs = {}

# Lock for thread safety when accessing jobs dictionary
jobs_lock = threading.Lock()

# Load CSV files at startup
@app.on_event("startup")
async def startup_event():
    global apollo_df, personalized_messages_df
    try:
        logger.info("Loading apollo.csv at startup...")
        apollo_df = pd.read_csv("apollo-contacts-export.csv")
        # Validate required columns
        required_columns = ["Email", "First Name", "Last Name", "Person Linkedin Url"]
        for col in required_columns:
            if col not in apollo_df.columns:
                logger.error(f"The '{col}' column is not found in apollo.csv")
                raise ValueError(f"The '{col}' column is not found in apollo.csv")
        logger.info("Successfully loaded apollo.csv")

        logger.info("Loading personalized_messages.csv at startup...")
        personalized_messages_df = pd.read_csv("personalized_messages.csv")
        # Validate required columns (updated expected column name)
        required_columns = ["Email", "Personalized_Message"]
        for col in required_columns:
            if col not in personalized_messages_df.columns:
                logger.error(f"The '{col}' column is not found in personalized_messages.csv")
                raise ValueError(f"The '{col}' column is not found in personalized_messages.csv")
        logger.info("Successfully loaded personalized_messages.csv")
    except Exception as e:
        logger.error(f"Failed to load CSV files: {str(e)}")
        raise Exception(f"Failed to load CSV files: {str(e)}")

def get_leads_page(campaign_id: str, starting_after: Optional[str] = None) -> Dict[str, Any]:
    """Fetch a single page of leads from the instantly.ai API"""
    url = "https://api.instantly.ai/api/v2/leads/list"
    body = {
        "campaign": campaign_id,
        "filter": "FILTER_VAL_OPENED_NO_REPLY",
    }
    
    if starting_after:
        body["starting_after"] = starting_after
        
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer NzFhYjkxN2ItNTlhYy00MTUzLWI2NzUtN2IwMGIzODhlOTI1Ok5yTXFzcGV4WVFNYw=="
    }
    
    try:
        response = requests.post(url, json=body, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching leads for campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching leads for campaign {campaign_id}: {str(e)}")

def get_all_leads(campaign_id: str) -> List[Dict[str, Any]]:
    """Fetch all leads for a campaign (used in original synchronous implementation)"""
    all_leads = []
    starting_after = None

    while True:
        data = get_leads_page(campaign_id, starting_after)
        all_leads.extend(data["items"])
        starting_after = data.get("next_starting_after")
        if not starting_after:
            break
        logger.info(f"Fetched {len(data['items'])} leads for campaign {campaign_id} in this page. Total so far: {len(all_leads)}")

    return all_leads

def process_leads_chunk(leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process a chunk of leads, matching them with apollo data and personalized messages"""
    global apollo_df, personalized_messages_df
    
    # Extract emails from leads
    api_emails = [lead["email"] for lead in leads if "email" in lead]
    if not api_emails:
        return []
        
    # Remove duplicates and create DataFrame
    api_emails = list(set(api_emails))
    api_df = pd.DataFrame(api_emails, columns=["Email"])
    
    # Standardize email columns (case-insensitive)
    api_df["Email"] = api_df["Email"].str.lower()
    
    # Merge API leads with apollo.csv
    matched_df = pd.merge(
        api_df[["Email"]],
        apollo_df[["Email", "First Name", "Last Name", "Person Linkedin Url"]],
        on="Email",
        how="inner"
    )
    
    if matched_df.empty:
        return []
    
    # Create "Name" from "First Name" and "Last Name"
    matched_df["First Name"] = matched_df["First Name"].fillna("")
    matched_df["Last Name"] = matched_df["Last Name"].fillna("")
    matched_df["Name"] = matched_df["First Name"] + " " + matched_df["Last Name"]
    
    # Merge with personalized_messages.csv
    matched_df = pd.merge(
        matched_df,
        personalized_messages_df[["Email", "Personalized_Message"]],
        on="Email",
        how="left"
    )
    
    # Set "InputField" to the personalized message
    matched_df["InputField"] = matched_df["Personalized_Message"].fillna("")
    
    # Prepare the final output
    output_df = matched_df[["Name", "Person Linkedin Url", "InputField"]].rename(columns={"Person Linkedin Url": "LinkedIn"})
    output_df["LinkedIn"] = output_df["LinkedIn"].fillna("")
    
    # Convert to list of dictionaries
    return output_df.to_dict(orient="records")

# Health check endpoint to verify the server is running
@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Server is running"}

# Background job processing function
def process_job(job_id: str, campaign_ids: List[str]):
    """Background function to process job asynchronously"""
    start_time = time.time()
    
    with jobs_lock:
        if job_id not in jobs:
            logger.error(f"Job {job_id} not found")
            return
            
        jobs[job_id]["status"] = "processing"
        jobs[job_id]["message"] = "Processing campaigns..."
        jobs[job_id]["last_updated"] = datetime.now().isoformat()
    
    try:
        all_results = []
        campaign_progress = {}
        total_processed = 0
        
        # Process each campaign
        for campaign_id in campaign_ids:
            campaign_start = time.time()
            
            with jobs_lock:
                jobs[job_id]["progress"][campaign_id] = {
                    "status": "processing",
                    "leads_fetched": 0,
                    "leads_processed": 0,
                    "message": "Starting to fetch leads..."
                }
                jobs[job_id]["last_updated"] = datetime.now().isoformat()
            
            # Process campaign in chunks with pagination
            starting_after = None
            campaign_leads = []
            page_count = 0
            
            while True:
                # Fetch one page of leads
                try:
                    page_count += 1
                    data = get_leads_page(campaign_id, starting_after)
                    page_leads = data["items"]
                    campaign_leads.extend(page_leads)
                    
                    # Update job status with progress
                    with jobs_lock:
                        jobs[job_id]["progress"][campaign_id]["leads_fetched"] = len(campaign_leads)
                        jobs[job_id]["progress"][campaign_id]["message"] = f"Fetched {len(campaign_leads)} leads (page {page_count})"
                        jobs[job_id]["last_updated"] = datetime.now().isoformat()
                    
                    # Process this batch
                    results = process_leads_chunk(page_leads)
                    if results:
                        all_results.extend(results)
                        
                        # Update results in the job object
                        with jobs_lock:
                            jobs[job_id]["results"] = all_results
                            jobs[job_id]["total_leads_found"] = len(all_results)
                            jobs[job_id]["last_updated"] = datetime.now().isoformat()
                    
                    # Update processed count
                    total_processed += len(page_leads)
                    
                    with jobs_lock:
                        jobs[job_id]["progress"][campaign_id]["leads_processed"] = len(page_leads)
                        jobs[job_id]["total_leads_processed"] = total_processed
                        jobs[job_id]["last_updated"] = datetime.now().isoformat()
                    
                    # Check if there are more pages
                    starting_after = data.get("next_starting_after")
                    if not starting_after:
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing campaign {campaign_id} in job {job_id}: {str(e)}")
                    with jobs_lock:
                        jobs[job_id]["progress"][campaign_id]["status"] = "error"
                        jobs[job_id]["progress"][campaign_id]["message"] = f"Error: {str(e)}"
                        jobs[job_id]["last_updated"] = datetime.now().isoformat()
                    break
            
            # Campaign completed
            campaign_time = time.time() - campaign_start
            with jobs_lock:
                jobs[job_id]["progress"][campaign_id]["status"] = "completed"
                jobs[job_id]["progress"][campaign_id]["message"] = f"Completed in {campaign_time:.1f}s"
                jobs[job_id]["progress"][campaign_id]["processing_time"] = campaign_time
                jobs[job_id]["last_updated"] = datetime.now().isoformat()
        
        # All campaigns processed
        processing_time = time.time() - start_time
        
        with jobs_lock:
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["message"] = f"All campaigns processed successfully in {processing_time:.1f}s"
            jobs[job_id]["processing_time"] = processing_time
            jobs[job_id]["last_updated"] = datetime.now().isoformat()
            
    except Exception as e:
        logger.error(f"Error in job {job_id}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["message"] = f"Error processing job: {str(e)}"
            jobs[job_id]["processing_time"] = time.time() - start_time
            jobs[job_id]["last_updated"] = datetime.now().isoformat()

# Create a new job
@app.post("/create-job/", response_model=JobResponse)
async def create_job(request: JobRequest):
    try:
        # Generate a unique job ID
        job_id = str(uuid.uuid4())
        created_at = datetime.now().isoformat()
        
        # Create job record
        with jobs_lock:
            jobs[job_id] = {
                "job_id": job_id,
                "status": "initializing",
                "message": "Job created, starting processing...",
                "campaign_ids": request.campaign_ids,
                "progress": {campaign_id: {"status": "pending"} for campaign_id in request.campaign_ids},
                "results": [],
                "total_leads_processed": 0,
                "total_leads_found": 0,
                "processing_time": 0,
                "created_at": created_at,
                "last_updated": created_at
            }
        
        # Start processing in a background thread
        threading.Thread(
            target=process_job,
            args=(job_id, request.campaign_ids),
            daemon=True
        ).start()
        
        return {"job_id": job_id, "status": "initializing", "message": "Job created and processing started"}
        
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating job: {str(e)}")

# Get job status and results
@app.get("/job-status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    with jobs_lock:
        if job_id not in jobs:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Return a copy of the job status to avoid race conditions
        return dict(jobs[job_id])

@app.post("/match-leads/", response_model=List[LeadResponse])
async def match_leads(request: CampaignRequest):
    try:
        # Step 1: Fetch leads for all campaign IDs
        all_leads = []
        for campaign_id in request.campaign_ids:
            logger.info(f"Fetching leads for campaign ID: {campaign_id}...")
            leads = get_all_leads(campaign_id)
            all_leads.extend(leads)
            logger.info(f"Total leads fetched for campaign {campaign_id}: {len(leads)}")

        logger.info(f"Total number of leads fetched across all campaigns: {len(all_leads)}")

        # Check if any leads were fetched
        if not all_leads:
            raise HTTPException(status_code=404, detail="No leads were fetched from the API for any campaign ID.")

        # Step 2: Convert the API leads into a DataFrame
        api_emails = [lead["email"] for lead in all_leads if "email" in lead]
        if not api_emails:
            raise HTTPException(status_code=400, detail="No emails found in the API response.")

        # Remove duplicates and convert to DataFrame
        api_emails = list(set(api_emails))
        api_df = pd.DataFrame(api_emails, columns=["Email"])

        # Step 3: Use the pre-loaded apollo.csv data
        global apollo_df
        if apollo_df is None:
            raise HTTPException(status_code=500, detail="apollo.csv data is not loaded.")

        # Step 4: Standardize email column names and values
        api_df["Email"] = api_df["Email"].str.lower()
        apollo_df["Email"] = apollo_df["Email"].str.lower()

        # Step 5: Merge the API data with apollo.csv
        matched_df = pd.merge(
            api_df[["Email"]],
            apollo_df[["Email", "First Name", "Last Name", "Person Linkedin Url"]],
            on="Email",
            how="inner"
        )

        # Step 6: Check if any matches were found
        if matched_df.empty:
            raise HTTPException(status_code=404, detail="No matching emails were found between the API data and apollo.csv.")

        # Step 7: Format the output safely
        matched_df["First Name"] = matched_df["First Name"].fillna("")
        matched_df["Last Name"] = matched_df["Last Name"].fillna("")
        matched_df["Name"] = matched_df["First Name"] + " " + matched_df["Last Name"]
        matched_df = matched_df.rename(columns={"Person Linkedin Url": "LinkedIn"})
        matched_df["LinkedIn"] = matched_df["LinkedIn"].fillna("")
        matched_df["InputField"] = ""

        output_df = matched_df[["Name", "LinkedIn", "InputField"]]

        # Step 8: Convert to JSON response
        result = output_df.to_dict(orient="records")
        logger.info(f"Total matches found: {len(result)}")
        return result

    except Exception as e:
        logger.error(f"Error in match_leads endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.post("/match-leads-go/", response_model=List[LeadResponse])
async def match_leads_go(request: CampaignRequest):
    try:
        # Step 1: Fetch leads for all campaign IDs
        all_leads = []
        for campaign_id in request.campaign_ids:
            logger.info(f"Fetching leads for campaign ID: {campaign_id}...")
            leads = get_all_leads(campaign_id)
            all_leads.extend(leads)
            logger.info(f"Total leads fetched for campaign {campaign_id}: {len(leads)}")
        logger.info(f"Total number of leads fetched across all campaigns: {len(all_leads)}")

        # Check if any leads were fetched
        if not all_leads:
            raise HTTPException(status_code=404, detail="No leads were fetched from the API for any campaign ID.")

        # Step 2: Convert the API leads into a DataFrame
        api_emails = [lead["email"] for lead in all_leads if "email" in lead]
        if not api_emails:
            raise HTTPException(status_code=400, detail="No emails found in the API response.")
        api_emails = list(set(api_emails))  # Remove duplicates
        api_df = pd.DataFrame(api_emails, columns=["Email"])

        # Step 3: Verify that CSV data is loaded
        if apollo_df is None:
            raise HTTPException(status_code=500, detail="apollo.csv data is not loaded.")
        if personalized_messages_df is None:
            raise HTTPException(status_code=500, detail="personalized_messages.csv data is not loaded.")

        # Step 4: Standardize email columns to lowercase for case-insensitive matching
        api_df["Email"] = api_df["Email"].str.lower()
        apollo_df["Email"] = apollo_df["Email"].str.lower()
        personalized_messages_df["Email"] = personalized_messages_df["Email"].str.lower()

        # Step 5: Merge API leads with apollo.csv
        matched_df = pd.merge(
            api_df[["Email"]],
            apollo_df[["Email", "First Name", "Last Name", "Person Linkedin Url"]],
            on="Email",
            how="inner"
        )

        # Step 6: Check if any matches were found
        if matched_df.empty:
            raise HTTPException(status_code=404, detail="No matching emails were found between the API data and apollo.csv.")

        # Step 7: Create "Name" from "First Name" and "Last Name"
        matched_df["First Name"] = matched_df["First Name"].fillna("")
        matched_df["Last Name"] = matched_df["Last Name"].fillna("")
        matched_df["Name"] = matched_df["First Name"] + " " + matched_df["Last Name"]

        # Step 8: Merge with personalized_messages.csv using the updated column name
        matched_df = pd.merge(
            matched_df,
            personalized_messages_df[["Email", "Personalized_Message"]],
            on="Email",
            how="left"
        )

        # Set "InputField" to the personalized message, defaulting to an empty string if missing
        matched_df["InputField"] = matched_df["Personalized_Message"].fillna("")

        # Prepare the final output
        output_df = matched_df[["Name", "Person Linkedin Url", "InputField"]].rename(columns={"Person Linkedin Url": "LinkedIn"})
        output_df["LinkedIn"] = output_df["LinkedIn"].fillna("")

        # Step 9: Convert to a list of dictionaries and return the result
        result = output_df.to_dict(orient="records")
        logger.info(f"Total matches found: {len(result)}")
        return result

    except Exception as e:
        logger.error(f"Error in match_leads_go endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3070)