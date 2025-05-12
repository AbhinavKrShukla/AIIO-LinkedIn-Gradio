import gradio as gr
import requests
import pandas as pd
import time
import json
import os
import threading
from typing import List, Dict, Any, Optional, Tuple

# Define campaign IDs (same as in the extension)
CAMPAIGN_IDS = [
    "ad2cbb80-59a4-4596-8ba6-229528d78b10",
    "06005835-0b5c-4bde-bf77-1d759738bc20",
    "4b9dddb4-c737-4e3e-bd33-9526acdd5dc9",
    "516546a4-e02b-4282-8228-a807493ba9a5"
]

# API configuration
API_BASE_URL = 'http://localhost:3070'
ITEMS_PER_PAGE = 5

# Global state variables
current_page = 1
all_data = []
request_id = None
processing_status = "idle"  # idle, initializing, processing, completed, error

# Custom CSS for styling the app to match the extension
custom_css = """
body {
    font-family: Arial, sans-serif;
    background-color: #f5f5f5;
    color: #333;
}
.container {
    margin: 10px;
}
h1 {
    text-align: center;
    color: #0a66c2; /* LinkedIn blue with better contrast */
    margin-bottom: 20px;
    font-weight: bold;
    text-shadow: 1px 1px 1px rgba(0,0,0,0.05);
}
table {
    width: 100%;
    border-collapse: collapse;
    margin: 20px 0;
    background-color: white;
    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
}
th, td {
    border: 1px solid #ddd;
    padding: 12px;
    text-align: left;
}
td {
    background-color: white;
    color: #333;
}
th {
    background-color: #0a66c2; /* LinkedIn blue with better contrast */
    color: white;
    font-weight: bold;
}
.copy-btn {
    padding: 6px 12px;
    margin-left: 8px;
    margin-top: 5px;
    cursor: pointer;
    background-color: #0a66c2; /* LinkedIn blue with better contrast */
    color: white;
    border: none;
    border-radius: 3px;
    font-weight: bold;
    box-shadow: 0 1px 2px rgba(0,0,0,0.1);
}
.copy-btn:hover {
    background-color: #004182; /* Darker LinkedIn blue with better contrast */
}
.pagination {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 15px;
    margin-top: 20px;
}
.pagination button {
    padding: 8px 15px;
    cursor: pointer;
    background-color: #0a66c2;
    color: white;
    border: none;
    border-radius: 3px;
    font-weight: bold;
    box-shadow: 0 1px 2px rgba(0,0,0,0.1);
}
.pagination button:hover {
    background-color: #004182;
}
.input-field {
    width: 100%;
    padding: 10px;
    border: 1px solid #bbb;
    border-radius: 3px;
    font-size: 16px;
    color: #333; /* Darker text for better visibility */
    background-color: #f9f9f9;
    box-shadow: inset 0 1px 2px rgba(0,0,0,0.05);
}
#loading-container {
    padding: 15px;
    background-color: #0a66c2;
    color: white;
    border-radius: 4px;
    margin-bottom: 20px;
    text-align: center;
    font-weight: bold;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.linkedin-url {
    color: #0a66c2;
    word-break: break-all;
    font-size: 14px;
}
.linkedin-url a {
    color: #0a66c2;
    text-decoration: underline;
}
.status-message {
    font-size: 16px;
    font-weight: bold;
    padding: 12px;
    background-color: #e8f0fe;
    color: #333;
    border-radius: 4px;
    margin-bottom: 15px;
    text-align: center;
    border: 1px solid #d4e5ff;
    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}
.error-message {
    color: #c62828; /* Darker red for better contrast */
    background-color: #ffebee;
    border-color: #ffcdd2;
}
.gr-button-primary {
    background-color: #0a66c2 !important;
    color: white !important;
}
.gr-button-secondary {
    background-color: #f3f3f3 !important;
    color: #333 !important;
    border: 1px solid #ddd !important;
}
.gr-form {
    background-color: white !important;
    box-shadow: 0 2px 5px rgba(0,0,0,0.1) !important;
    border-radius: 5px !important;
    padding: 20px !important;
}
"""

# JavaScript for copy functionality - will be included in the HTML

def escape_html(text: str) -> str:
    """Safely escape HTML attribute values"""
    if not isinstance(text, str):
        return ''
    return (text
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#039;'))

def start_processing() -> Dict[str, Any]:
    """Start processing campaigns on the server"""
    global request_id, processing_status
    
    processing_status = "initializing"
    
    try:
        # Simply signal that processing has started
        # The actual data fetching will happen in fetch_all_results
        
        # Start a background thread to fetch results
        threading.Thread(target=fetch_all_results, daemon=True).start()
        
        return {"status": "processing", "message": "Processing started. Please wait..."}
    except Exception as e:
        processing_status = "error"
        return {"error": str(e)}

def fetch_all_results() -> None:
    """Fetch all results at once from the match-leads-go endpoint"""
    global all_data, processing_status
    
    processing_status = "processing"
    
    try:
        print("Starting API request to fetch leads...")
        # Use the match-leads-go endpoint which includes personalized messages
        response = requests.post(
            f"{API_BASE_URL}/match-leads-go/",
            headers={'Content-Type': 'application/json', 'accept': 'application/json'},
            json={"campaign_ids": CAMPAIGN_IDS}
        )
        
        print(f"API response status code: {response.status_code}")
        
        if response.status_code != 200:
            error_message = f"HTTP error! status: {response.status_code}"
            try:
                error_data = response.json()
                if 'detail' in error_data:
                    error_message = f"API Error: {error_data['detail']}"
            except:
                pass
            
            print(error_message)
            processing_status = "error"
            return
        
        # The API returns a direct list, not a dictionary with 'items'
        data = response.json()
        print(f"Type of API response data: {type(data)}")
        
        # Print a sample of the data for debugging
        if data and len(data) > 0:
            print(f"First item in response: {data[0]}")
            print(f"Keys in first item: {data[0].keys() if isinstance(data[0], dict) else 'Not a dictionary'}")
        else:
            print("API returned empty data list")
        
        # Clean the data to ensure consistent format
        cleaned_items = []
        for item in data:
            # Handle potential None or empty values
            input_field = item.get("InputField", "") or ""
            linkedin_url = item.get("LinkedIn", "") or ""
            name = item.get("Name", "") or ""
            
            # Debug current item
            print(f"Processing item - Name: '{name}', LinkedIn: '{linkedin_url}', InputField: '{input_field}'")
            
            # Remove quotes if they're wrapping the input field
            if isinstance(input_field, str) and input_field.startswith('"') and input_field.endswith('"'):
                input_field = input_field[1:-1]
                print(f"  - Removed quotes from InputField: '{input_field}'")
            
            cleaned_items.append({
                "Name": name,
                "LinkedIn": linkedin_url,
                "InputField": input_field
            })
        
        all_data = cleaned_items
        processing_status = "completed"
        
        # Print sample of the processed data
        if all_data and len(all_data) > 0:
            print(f"First processed item: {all_data[0]}")
        
        # Print success message
        print(f"Successfully fetched {len(all_data)} leads from the API")
    except Exception as e:
        print(f"Error fetching results: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        processing_status = "error"

def create_table_html(data: List[Dict[str, str]], page: int) -> str:
    """Create HTML table for the current page of data"""
    if not data:
        return "<p>Waiting for data...</p>"
    
    # Calculate pagination
    total_items = len(data)
    total_pages = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    
    # Ensure current page is valid
    page = max(1, min(page, total_pages))
    
    # Get data for current page
    start = (page - 1) * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total_items)
    page_data = data[start:end]
    
    # Create table HTML with embedded JavaScript for copy functionality
    table_html = """
    <table>
        <tr>
            <th>Name</th>
            <th>LinkedIn Profile</th>
            <th>Input Field</th>
        </tr>
    """
    
    for item in page_data:
        name = escape_html(item.get("Name", ""))
        linkedin_url = escape_html(item.get("LinkedIn", ""))
        input_field = escape_html(item.get("InputField", ""))
        
        table_html += f"""
        <tr>
            <td>{name}</td>
            <td>
                <div class="linkedin-url">
                    <a href="{linkedin_url}" target="_blank">{linkedin_url}</a>
                </div>
                <button class="copy-btn" onclick="copyToClipboard('{linkedin_url}')">Copy URL</button>
            </td>
            <td>
                <textarea class="input-field" id="input-{start + page_data.index(item)}" rows="4">{input_field}</textarea>
                <button class="copy-btn" onclick="copyToClipboard(document.getElementById('input-{start + page_data.index(item)}').value)">Copy</button>
            </td>
        </tr>
        """
    
    table_html += "</table>"
    return table_html

def get_status_html() -> str:
    """Get HTML for the current processing status"""
    global processing_status
    
    if processing_status == "idle":
        return "<div class='status-message'>Click 'Process' to start loading data.</div>"
    elif processing_status == "initializing":
        return "<div class='status-message'>Initializing... Please wait.</div>"
    elif processing_status == "processing":
        return "<div class='status-message'>Processing... Please wait. This may take a few minutes.</div>"
    elif processing_status == "completed":
        return f"<div class='status-message'>Completed! Found {len(all_data)} records.</div>"
    else:  # error
        return "<div class='status-message error-message'>An error occurred. Check the console for details and try again.</div>"

def get_page_info(page: int) -> str:
    """Get pagination information"""
    total_items = len(all_data)
    total_pages = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    return f"Page {page} of {total_pages}"

def update_ui(page: int = 1) -> Tuple[str, str, str]:
    """Update the UI components based on current state"""
    global all_data, processing_status
    
    # Get table HTML
    table_html = create_table_html(all_data, page)
    
    # Get status HTML
    status_html = get_status_html()
    
    # Get page info
    page_info = get_page_info(page)
    
    # Note: We're not returning the button states anymore as they're not used in the UI
    # Just update the UI elements that are being displayed
    return table_html, status_html, page_info

def on_prev_click(page: int) -> int:
    """Handle previous button click"""
    return max(1, page - 1)

def on_next_click(page: int) -> int:
    """Handle next button click"""
    # Calculate total pages
    total_items = len(all_data)
    total_pages = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    return min(page + 1, total_pages)

def on_refresh_click(page: int) -> int:
    """Handle refresh button click"""
    # Just return the current page, then update_ui will be called in the .then() chain
    return page

def create_app() -> gr.Blocks:
    """Create the Gradio app"""
    with gr.Blocks(css=custom_css) as app:
        gr.HTML("<h1>LinkedIn Personalized Messages</h1>")
        
        with gr.Row():
            status_html = gr.HTML(get_status_html(), elem_id="status-html")
        
        with gr.Row():
            process_button = gr.Button("Process Campaigns")
            refresh_button = gr.Button("Refresh Data")
        
        with gr.Row():
            prev_button = gr.Button("Previous Page")
            page_info = gr.HTML(get_page_info(1), elem_id="page-info")
            next_button = gr.Button("Next Page")
        
        with gr.Row():
            table_html = gr.HTML("", elem_id="table-html")
        
        # Add JavaScript for client-side functionality
        gr.HTML("""
        <script>
            function copyToClipboard(text) {
                const tempInput = document.createElement('textarea');
                tempInput.value = text;
                document.body.appendChild(tempInput);
                tempInput.select();
                document.execCommand('copy');
                document.body.removeChild(tempInput);
                
                // Show copy notification
                const notification = document.createElement('div');
                notification.textContent = 'Copied!';
                notification.style.position = 'fixed';
                notification.style.bottom = '20px';
                notification.style.right = '20px';
                notification.style.padding = '10px';
                notification.style.backgroundColor = '#4CAF50';
                notification.style.color = 'white';
                notification.style.borderRadius = '4px';
                notification.style.zIndex = '1000';
                document.body.appendChild(notification);
                
                setTimeout(() => {
                    notification.style.opacity = '0';
                    notification.style.transition = 'opacity 0.5s';
                    setTimeout(() => document.body.removeChild(notification), 500);
                }, 1500);
            }
        </script>
        """)
        
        # Define page state
        page_state = gr.State(1)
        
        # Function to check status and update UI
        def check_status(page):
            return update_ui(page)
        
        # Button handlers
        process_button.click(
            fn=start_processing,
            inputs=[],
            outputs=[status_html]
        ).then(
            fn=lambda: time.sleep(1),
            inputs=[],
            outputs=[]
        ).then(
            fn=update_ui,
            inputs=[page_state],
            outputs=[table_html, status_html, page_info]
        )
        
        refresh_button.click(
            fn=lambda page: update_ui(page),
            inputs=[page_state],
            outputs=[table_html, status_html, page_info]
        )
        
        prev_button.click(
            fn=on_prev_click,
            inputs=[page_state],
            outputs=[page_state]
        ).then(
            fn=update_ui,
            inputs=[page_state],
            outputs=[table_html, status_html, page_info]
        )
        
        next_button.click(
            fn=on_next_click,
            inputs=[page_state],
            outputs=[page_state]
        ).then(
            fn=update_ui,
            inputs=[page_state],
            outputs=[table_html, status_html, page_info]
        )
        
        # Add a note about the need to refresh manually
        with gr.Row():
            gr.HTML("<p><i>Note: Please click 'Refresh Data' periodically to check processing status.</i></p>")
        
        # Update UI when the app loads
        app.load(
            fn=update_ui,
            inputs=[page_state],
            outputs=[table_html, status_html, page_info]
        )
    
    return app

if __name__ == "__main__":
    app = create_app()
    app.launch(share=True)
