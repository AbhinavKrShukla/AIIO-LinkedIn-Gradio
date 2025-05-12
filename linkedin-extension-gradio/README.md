# LinkedIn Data Viewer - Gradio Alternative

This is a Gradio-based alternative to the LinkedIn Chrome extension. It provides the same functionality but as a web application that can be accessed from any browser without requiring installation of a Chrome extension.

## Features

- Connects to the same backend API as the Chrome extension
- Displays LinkedIn profiles with copy functionality
- Provides a paginated interface for viewing results
- Real-time status updates during processing
- Copy-to-clipboard functionality for LinkedIn URLs and input fields

## Setup Instructions

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Make sure the backend API server is running at http://localhost:3070

3. Run the Gradio application:

```bash
python app.py
```

4. Open your browser and navigate to the URL displayed in the terminal (typically http://127.0.0.1:7860)

## How It Works

The application connects to the same API endpoints as the Chrome extension:
- `http://localhost:3070/match-leads/` - For fetching lead data
- `http://localhost:3070/processing-status/` - For status updates

The UI is designed to closely match the Chrome extension's interface, with the same functionality:
- View LinkedIn profiles
- Copy LinkedIn URLs with a single click
- Copy input field values
- Navigate through pages of results

## Configuration

You can modify the following variables in `app.py` to customize the application:

- `CAMPAIGN_IDS` - The list of campaign IDs to process
- `API_BASE_URL` - The base URL for the API server
- `ITEMS_PER_PAGE` - The number of items to display per page
