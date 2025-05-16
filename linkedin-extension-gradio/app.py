import gradio as gr
import requests
import pandas as pd
import time
import json

# Define campaign IDs (same as in the extension)
CAMPAIGN_IDS = [
    "ad2cbb80-59a4-4596-8ba6-229528d78b10",
    "06005835-0b5c-4bde-bf77-1d759738bc20",
    "4b9dddb4-c737-4e3e-bd33-9526acdd5dc9",
    "516546a4-e02b-4282-8228-a807493ba9a5"
]

# API configuration
API_BASE_URL = 'http://localhost:3070'
ITEMS_PER_PAGE = 10

# Global variables to store job state
job_id = None
job_status = "idle"  # idle, processing, completed, error
current_page = 1

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
    vertical-align: top;
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
/* Column widths */
.col-name {
    width: 20%;
}
.col-linkedin {
    width: 15%;
}
.col-message {
    width: 65%;
}
/* Icon buttons */
.icon-btn {
    width: 32px;
    height: 32px;
    min-width: 32px;
    min-height: 32px;
    max-width: 32px;
    max-height: 32px;
    border-radius: 50%;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    font-size: 18px;
    background: #e8f3fc;
    border: none;
    cursor: pointer;
    position: relative;
    transition: background 0.2s, color 0.2s, font-size 0.2s;
    padding: 0;
    vertical-align: middle;
    text-align: center;
    white-space: nowrap;
}
.icon-btn:active, .icon-btn:focus {
    outline: none;
    background: #d0e7fa;
}
.icon-btn.copied {
    font-size: 12px !important;
    color: #0a66c2 !important;
}

    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border-radius: 50%;
    background-color: #0a66c2;
    color: white;
    border: none;
    cursor: pointer;
    margin: 0 5px;
    font-size: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.2);
}
.icon-btn:hover {
    background-color: #004182;
}
.icon-copy::before {
    content: '📋';
}
.icon-link::before {
    content: '🔗';
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
.status {
    padding: 15px;
    margin: 10px 0;
    border-radius: 5px;
    font-weight: bold;
    color: #333;
}
.status-processing {
    background-color: #e3f2fd;
    border: 1px solid #2196f3;
    color: #0d47a1;
}
.status-completed {
    background-color: #e8f5e9;
    border: 1px solid #4caf50;
    color: #1b5e20;
}
.status-error {
    background-color: #ffebee;
    border: 1px solid #f44336;
    color: #b71c1c;
}
.linkedin-url {
    color: #0a66c2;
    font-size: 14px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 150px;
    display: block;
}
.linkedin-url a {
    color: #0a66c2;
    text-decoration: underline;
}
.message-textarea {
    width: 100%;
    height: 80px;
    padding: 8px;
    border: 1px solid #ddd;
    border-radius: 4px;
    resize: vertical;
    color: #333;
    background-color: #f9f9f9;
}
.button-container {
    display: flex;
    justify-content: flex-end;
    margin-top: 5px;
}
"""

def start_job():
    """Start a new job on the server"""
    global job_id, job_status
    
    try:
        # Create a new job
        response = requests.post(
            f"{API_BASE_URL}/create-job/",
            headers={'Content-Type': 'application/json'},
            json={"campaign_ids": CAMPAIGN_IDS}
        )
        
        if response.status_code != 200:
            job_status = "error"
            return f"Error starting job: HTTP {response.status_code}"
        
        # Get job ID from response
        data = response.json()
        job_id = data.get("job_id")
        
        if not job_id:
            job_status = "error"
            return "Error: No job ID returned from server"
        
        job_status = "processing"
        return f"Job started with ID: {job_id}"
    
    except Exception as e:
        job_status = "error"
        return f"Error: {str(e)}"

def get_job_status():
    """Get the current status of the job"""
    global job_id, job_status
    
    if not job_id:
        return "No job has been started"
    
    try:
        response = requests.get(
            f"{API_BASE_URL}/job-status/{job_id}",
            headers={'accept': 'application/json'}
        )
        
        if response.status_code != 200:
            return f"Error checking job status: HTTP {response.status_code}"
        
        data = response.json()
        status = data.get("status", "unknown")
        message = data.get("message", "")
        total_processed = data.get("total_leads_processed", 0)
        total_found = data.get("total_leads_found", 0)
        
        job_status = status
        
        return f"Status: {status}\nMessage: {message}\nProcessed: {total_processed}\nFound: {total_found}"
    
    except Exception as e:
        return f"Error checking status: {str(e)}"

def escape_html(text):
    """Safely escape HTML special characters"""
    if not isinstance(text, str):
        return ''
    return (text
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#039;'))

def create_table_html(results, page=1):
    """Create HTML table with icon buttons for copying and opening links"""
    if not results:
        return "<div>No results available yet</div>"
    
    # Calculate pagination
    total_items = len(results)
    total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, total_items)
    
    # Get the current page of data
    page_data = results[start_idx:end_idx]
    
    # Create the HTML table
    table_html = "<table>"
    
    # Table header
    table_html += "<thead><tr>"
    table_html += "<th class='col-name'>Name</th>"
    table_html += "<th class='col-linkedin'>LinkedIn</th>"
    table_html += "<th class='col-message'>Personalized Message</th>"
    table_html += "</tr></thead>"
    
    # Table body
    table_html += "<tbody>"
    
    if not page_data:
        # No data available
        table_html += "<tr><td colspan='3' style='text-align: center;'>No data available yet.</td></tr>"
    else:
        # Add rows for each item in the current page
        for i, item in enumerate(page_data):
            name = escape_html(item.get("Name", ""))
            linkedin_url = escape_html(item.get("LinkedIn", ""))
            input_field = escape_html(item.get("InputField", ""))
            
            table_html += "<tr>"
            
            # Name column
            table_html += f"<td class='col-name'>{name}</td>"
            
            # LinkedIn URL column with icon buttons
            table_html += "<td class='col-linkedin'>"
            if linkedin_url:
                # Show truncated URL with buttons
                table_html += f"<div class='linkedin-url'>{linkedin_url}</div>"
                table_html += "<div class='button-container'>"
                # Copy URL button
                table_html += f"<button class='icon-btn icon-copy copy-btn' data-text='{linkedin_url}' title='Copy LinkedIn URL'></button>"
                # Open URL button
                table_html += f"<a href='{linkedin_url}' target='_blank'><button class='icon-btn icon-link' title='Open LinkedIn Profile'></button></a>"
                table_html += "</div>"
            else:
                table_html += "-"
            table_html += "</td>"
            
            # Personalized message column with textarea and copy button
            table_html += "<td class='col-message'>"
            if input_field:
                table_html += f"<textarea class='message-textarea' readonly>{input_field}</textarea>"
                table_html += "<div class='button-container'>"
                table_html += f"<button class='icon-btn icon-copy copy-btn' data-text='{input_field}' title='Copy Message'></button>"
                table_html += "</div>"
            else:
                table_html += "-"
            table_html += "</td>"
            
            table_html += "</tr>"
    
    table_html += "</tbody>"
    table_html += "</table>"
    # Use inline onclick handler for copy with fallback
    copy_inline = ("onclick=\"(async function(btn){"
        "const text = btn.getAttribute('data-text');"
        "try { await navigator.clipboard.writeText(text); }"
        "catch (err) {"
        "  var textarea = document.createElement('textarea');"
        "  textarea.value = text;"
        "  document.body.appendChild(textarea);"
        "  textarea.select();"
        "  try { document.execCommand('copy'); } catch(e) {}"
        "  document.body.removeChild(textarea);"
        "}"
        "const orig = btn.innerText;"
        "btn.classList.add('copied');"
        "btn.innerText='Copied!';"
        "setTimeout(()=>{btn.innerText=orig; btn.classList.remove('copied');},1500);"
        "})(this)\"")
    # Patch the copy button HTMLs to include the inline handler
    table_html = table_html.replace(
        ">\u003cbutton class='icon-btn icon-copy copy-btn' ",
        f">\u003cbutton class='icon-btn icon-copy copy-btn' {copy_inline} "
    )
    return table_html



def get_results(page=1):
    """Get the current results from the job"""
    global job_id
    
    if not job_id:
        return "No job has been started", ""
    
    try:
        response = requests.get(
            f"{API_BASE_URL}/job-status/{job_id}",
            headers={'accept': 'application/json'}
        )
        
        if response.status_code != 200:
            return f"Error fetching results: HTTP {response.status_code}", ""
        
        data = response.json()
        results = data.get("results", [])
        
        if not results:
            return "No results available yet", ""
        
        # Calculate pagination
        total_items = len(results)
        total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        page = max(1, min(page, total_pages))
        
        start_idx = (page - 1) * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, total_items)
        
        # Get page info
        page_info = f"Page {page} of {total_pages} (Showing {start_idx+1}-{end_idx} of {total_items} results)"
        
        # Create HTML table
        table_html = create_table_html(results, page)
        
        # Return the page info and table HTML
        return page_info, table_html
    
    except Exception as e:
        return f"Error fetching results: {str(e)}", ""

def update_ui():
    """Update all UI components"""
    status_text = get_job_status()
    page_info, results_html = get_results(current_page)
    
    # Determine status class
    status_class = "status "
    if job_status == "processing":
        status_class += "status-processing"
    elif job_status == "completed":
        status_class += "status-completed"
    elif job_status == "error":
        status_class += "status-error"
    
    # Create status HTML
    status_html = f"<div class='{status_class}'>{status_text}</div>"
    
    # Return the same page_info for both top and bottom pagination
    return status_html, page_info, results_html, page_info

def on_prev_click():
    """Handle previous page button click"""
    global current_page
    current_page = max(1, current_page - 1)
    return update_ui()

def on_next_click():
    """Handle next page button click"""
    global current_page
    # Get job status to check total results
    try:
        response = requests.get(f"{API_BASE_URL}/job-status/{job_id}", headers={'accept': 'application/json'})
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            total_items = len(results)
            total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            
            # Only increment if there are more pages
            if current_page < total_pages:
                current_page += 1
    except:
        pass  # If there's an error, just don't increment the page
    
    return update_ui()

def download_csv():
    """Function to return the CSV file for download"""
    csv_path = "filtered_personalized_messages_theGo.csv"
    return csv_path

def create_app():
    """Create the Gradio application"""
    with gr.Blocks(css=custom_css, theme=gr.themes.Soft()) as app:
        gr.HTML("<h1>LinkedIn Personalized Messages</h1>")
        
        with gr.Row():
            with gr.Column():
                # Download button at the top left
                download_button = gr.Button("📥 Download CSV", variant="primary", size="sm")
                file_output = gr.File(visible=False)
                download_button.click(fn=download_csv, inputs=None, outputs=file_output)
                
                # Add some space after the download button
                gr.HTML("<div style='height: 10px;'></div>")
                # Buttons
                with gr.Row():
                    start_button = gr.Button("Start Processing", variant="primary")
                    refresh_button = gr.Button("Refresh Status", variant="secondary")
                
                # Status display
                status_html = gr.HTML("<div class='status'>Click 'Start Processing' to begin</div>")
                
                # Top pagination controls
                with gr.Row():
                    prev_button_top = gr.Button("Previous Page")
                    page_info = gr.HTML("Page 1")
                    next_button_top = gr.Button("Next Page")
                
                # Results display (using HTML instead of DataFrame)
                results_html = gr.HTML("<div>No results yet</div>")
                
                # Bottom pagination controls
                with gr.Row():
                    prev_button_bottom = gr.Button("Previous Page")
                    page_info_bottom = gr.HTML("Page 1")
                    next_button_bottom = gr.Button("Next Page")
        
        # Button click handlers
        start_button.click(
            fn=start_job,
            inputs=None,
            outputs=status_html
        ).then(
            fn=update_ui,
            inputs=None,
            outputs=[status_html, page_info, results_html, page_info_bottom]
        )
        
        refresh_button.click(
            fn=update_ui,
            inputs=None,
            outputs=[status_html, page_info, results_html, page_info_bottom]
        )
        
        # Top pagination buttons
        prev_button_top.click(
            fn=on_prev_click,
            inputs=None,
            outputs=[status_html, page_info, results_html, page_info_bottom]
        )
        
        next_button_top.click(
            fn=on_next_click,
            inputs=None,
            outputs=[status_html, page_info, results_html, page_info_bottom]
        )
        
        # Bottom pagination buttons
        prev_button_bottom.click(
            fn=on_prev_click,
            inputs=None,
            outputs=[status_html, page_info, results_html, page_info_bottom]
        )
        
        next_button_bottom.click(
            fn=on_next_click,
            inputs=None,
            outputs=[status_html, page_info, results_html, page_info_bottom]
        )
    
    return app

if __name__ == "__main__":
    app = create_app()
    app.launch(server_name="0.0.0.0", server_port=7860)
