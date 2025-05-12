import requests
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import logging

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

# Pydantic model for the request body
class CampaignRequest(BaseModel):
    campaign_ids: List[str]

# Pydantic model for the response
class LeadResponse(BaseModel):
    Name: str
    LinkedIn: str
    InputField: str

# Global variables to store CSV data
apollo_df = None
personalized_messages_df = None

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

def get_all_leads(campaign_id: str) -> List[Dict[str, Any]]:
    url = "https://api.instantly.ai/api/v2/leads/list"
    body = {
        "campaign": campaign_id,
        "filter": "FILTER_VAL_OPENED_NO_REPLY",
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer NzFhYjkxN2ItNTlhYy00MTUzLWI2NzUtN2IwMGIzODhlOTI1Ok5yTXFzcGV4WVFNYw=="
    }

    all_leads = []
    starting_after = None

    while True:
        if starting_after:
            body["starting_after"] = starting_after

        try:
            response = requests.post(url, json=body, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_leads.extend(data["items"])
            starting_after = data.get("next_starting_after")
            if not starting_after:
                break
            logger.info(f"Fetched {len(data['items'])} leads for campaign {campaign_id} in this page. Total so far: {len(all_leads)}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching leads for campaign {campaign_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Error fetching leads for campaign {campaign_id}: {str(e)}")

    return all_leads

# Health check endpoint to verify the server is running
@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Server is running"}

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