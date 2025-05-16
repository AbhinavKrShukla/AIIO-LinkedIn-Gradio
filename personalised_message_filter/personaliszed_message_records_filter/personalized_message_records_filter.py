import csv
import os
import pandas as pd
from pathlib import Path

def filter_personalized_messages():
    # Define file paths
    base_dir = Path(__file__).parent
    personalized_messages_path = base_dir / "personalized_messages.csv"
    campaign_leads_dir = base_dir / "campaign_leads_extractor_to_csv" / "campaign_leads"
    output_path = base_dir / "filtered_personalized_messages.csv"
    
    # Check if the required files and directories exist
    if not personalized_messages_path.exists():
        print(f"Error: {personalized_messages_path} does not exist")
        return
    
    if not campaign_leads_dir.exists() or not campaign_leads_dir.is_dir():
        print(f"Error: {campaign_leads_dir} does not exist or is not a directory")
        return
    
    print(f"Reading personalized messages from: {personalized_messages_path}")
    
    # Read the personalized messages CSV file
    try:
        personalized_messages_df = pd.read_csv(personalized_messages_path)
        print(f"Found {len(personalized_messages_df)} records in personalized messages file")
    except Exception as e:
        print(f"Error reading personalized messages file: {e}")
        return
    
    # Get all campaign leads CSV files
    campaign_files = [f for f in campaign_leads_dir.glob("*.csv")]
    print(f"Found {len(campaign_files)} campaign lead CSV files")
    
    if not campaign_files:
        print("No campaign lead CSV files found")
        return
    
    # Create a set of all emails from campaign leads files
    campaign_emails = set()
    for campaign_file in campaign_files:
        try:
            campaign_df = pd.read_csv(campaign_file)
            if 'email' in campaign_df.columns:
                campaign_emails.update(campaign_df['email'].dropna().tolist())
        except Exception as e:
            print(f"Error reading campaign file {campaign_file.name}: {e}")
    
    print(f"Found {len(campaign_emails)} unique email addresses in campaign lead files")
    
    # Filter personalized messages based on email matches
    if 'Email' in personalized_messages_df.columns:
        filtered_df = personalized_messages_df[personalized_messages_df['Email'].isin(campaign_emails)]
        print(f"Found {len(filtered_df)} matching records")
        
        # Save the filtered records to a new CSV file
        filtered_df.to_csv(output_path, index=False)
        print(f"Filtered records saved to: {output_path}")
    else:
        print("Error: 'Email' column not found in personalized messages file")

if __name__ == "__main__":
    filter_personalized_messages()
