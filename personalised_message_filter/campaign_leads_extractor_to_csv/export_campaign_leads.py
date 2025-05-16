import requests
import pandas as pd
import logging
# import argparse  # Not needed anymore as we're using hardcoded values
import os
from typing import List, Dict, Any, Optional
import time
from datetime import datetime
import sys
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        raise Exception(f"Error fetching leads for campaign {campaign_id}: {str(e)}")

def get_and_save_leads(campaign_id: str, csv_writer, df_columns=None) -> int:
    """Fetch and save leads for a campaign instantaneously"""
    starting_after = None
    page_count = 0
    total_leads = 0
    is_first_page = True

    logger.info(f"Starting to fetch and save leads for campaign {campaign_id}")
    
    while True:
        page_count += 1
        data = get_leads_page(campaign_id, starting_after)
        page_leads = data["items"]
        total_leads += len(page_leads)
        
        logger.info(f"Fetched {len(page_leads)} leads for campaign {campaign_id} (page {page_count}). Total so far: {total_leads}")
        
        # Process and save this batch of leads immediately
        if page_leads:
            df = pd.DataFrame(page_leads)
            
            # If this is the first page, write the header and store the columns
            if is_first_page and df_columns is None:
                df.to_csv(csv_writer, index=False, mode='a', header=True)
                is_first_page = False
            else:
                # For subsequent pages, don't write the header
                # If df_columns is provided, ensure consistent column order
                if df_columns is not None:
                    # Ensure all columns exist, fill missing with NaN
                    for col in df_columns:
                        if col not in df.columns:
                            df[col] = pd.NA
                    # Reorder columns to match first page
                    df = df[df_columns]
                
                df.to_csv(csv_writer, index=False, mode='a', header=False)
        
        # Check if there are more pages
        starting_after = data.get("next_starting_after")
        if not starting_after:
            break
            
        # Optional: Add a small delay to avoid rate limiting
        time.sleep(0.5)

    logger.info(f"Completed fetching and saving all leads for campaign {campaign_id}. Total: {total_leads}")
    return total_leads

def create_csv_file(campaign_id: str, output_dir: str) -> str:
    """Create a CSV file for saving leads"""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(output_dir, f"campaign_{campaign_id}_{timestamp}.csv")
    
    # Create an empty file
    with open(filename, 'w') as f:
        pass
    
    logger.info(f"Created CSV file at {filename}")
    return filename

def process_campaigns(campaign_ids: List[str], output_dir: str) -> Dict[str, Any]:
    """Process multiple campaigns and save each to a CSV file instantaneously"""
    results = {}
    
    for campaign_id in campaign_ids:
        try:
            logger.info(f"Processing campaign {campaign_id}")
            start_time = time.time()
            
            # Create CSV file
            csv_path = create_csv_file(campaign_id, output_dir)
            
            # Get and save leads instantaneously
            with open(csv_path, 'a', newline='') as csv_file:
                leads_count = get_and_save_leads(campaign_id, csv_file)
            
            processing_time = time.time() - start_time
            
            if leads_count > 0:
                results[campaign_id] = {
                    "status": "success",
                    "leads_count": leads_count,
                    "csv_path": csv_path,
                    "processing_time": f"{processing_time:.2f} seconds"
                }
                
                logger.info(f"Campaign {campaign_id} processed successfully in {processing_time:.2f} seconds")
            else:
                results[campaign_id] = {
                    "status": "warning",
                    "message": "No leads found for this campaign",
                    "leads_count": 0,
                    "csv_path": csv_path
                }
                
        except Exception as e:
            logger.error(f"Error processing campaign {campaign_id}: {str(e)}")
            results[campaign_id] = {
                "status": "error",
                "message": str(e)
            }
    
    return results

def main():
    campaign_ids = [
        "ad2cbb80-59a4-4596-8ba6-229528d78b10",  
        "06005835-0b5c-4bde-bf77-1d759738bc20",  
        "4b9dddb4-c737-4e3e-bd33-9526acdd5dc9",  
        "516546a4-e02b-4282-8228-a807493ba9a5",  
    ]
    
    # Get the directory where the script is located
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    output_dir = script_dir
    
    logger.info(f"Starting export for {len(campaign_ids)} campaigns")
    logger.info(f"CSV files will be saved in: {output_dir}")
    results = process_campaigns(campaign_ids, output_dir)
    
    # Print summary
    print("\nExport Summary:")
    print("=" * 50)
    for campaign_id, result in results.items():
        status = result["status"]
        if status == "success":
            print(f"Campaign {campaign_id}: {result['leads_count']} leads exported to {result['csv_path']} in {result['processing_time']}")
        else:
            print(f"Campaign {campaign_id}: {status.upper()} - {result.get('message', 'Unknown error')}")
    
    print("=" * 50)
    print(f"Export completed. Files saved to: {output_dir}")

if __name__ == "__main__":
    main()
