document.addEventListener('DOMContentLoaded', () => {
  let currentPage = 1;
  const itemsPerPage = 5;
  let apiData = [];
  let isProcessing = false;
  let requestId = null;
  let eventSource = null;
  let errorCount = 0; // Track consecutive errors

  // Define campaign IDs array
  const campaignIds = [
    "ad2cbb80-59a4-4596-8ba6-229528d78b10",
    "06005835-0b5c-4bde-bf77-1d759738bc20",
    "4b9dddb4-c737-4e3e-bd33-9526acdd5dc9",
    "516546a4-e02b-4282-8228-a807493ba9a5"
  ];

  // API base URL
  const API_BASE_URL = 'http://localhost:3070';

  // Update status indicators
  const updateStatus = (status, processedCount, totalCampaigns, completedCampaigns) => {
    const loadingContainer = document.getElementById('loading-container');
    const loadingEl = document.getElementById('loading');
    const progressEl = document.getElementById('progress');
    
    if (!loadingContainer || !loadingEl || !progressEl) return;
    
    if (status === 'initializing') {
      loadingContainer.style.display = 'block';
      loadingEl.textContent = 'Initializing...';
      progressEl.textContent = 'Starting to process campaigns...';
    } else if (status === 'processing') {
      loadingContainer.style.display = 'block';
      loadingEl.textContent = 'Processing Leads...';
      progressEl.textContent = `Found ${processedCount} matching leads. Completed ${completedCampaigns.length}/${totalCampaigns} campaigns`;
    } else if (status === 'completed') {
      // Hide loading container when completed
      loadingContainer.style.display = 'none';
    } else if (status === 'error') {
      loadingContainer.style.display = 'block';
      loadingEl.textContent = 'Error!';
      progressEl.textContent = 'There was an error processing your request.';
    }
  };

  // Start processing campaigns and fetch data
  async function startProcessing() {
    try {
      // Show loading container
      const loadingContainer = document.getElementById('loading-container');
      if (loadingContainer) {
        loadingContainer.style.display = 'block';
      }
      
      // Update initial status
      updateStatus('initializing', 0, campaignIds.length, []);
      
      // We don't need to make a separate request here first
      // Just directly fetch the results
      fetchAllResults();
      
    } catch (error) {
      console.error('Error starting processing:', error);
      updateStatus('error', 0, campaignIds.length, []);
    }
  }

  function connectToEventStream() {
    if (!requestId) {
      console.error('No request ID available');
      return;
    }
    
    // Close any existing connection
    if (eventSource) {
      eventSource.close();
    }
    
    eventSource = new EventSource(`${API_BASE_URL}/processing-status/${requestId}`);
    
    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        // Update status indicators
        updateStatus(
          data.status, 
          data.processed_count, 
          campaignIds.length, 
          data.completed_campaigns || []
        );
        
        // Process any new leads
        if (data.results && data.results.length > 0) {
          try {
            // Prevent unnecessary rerenders if data is the same length
            const oldDataLength = apiData.length;
            
            // Clean the data before storing it
            const cleanResults = data.results.map(item => {
              // Get the input field value and strip quotes if they exist
              let inputField = item.InputField || "";
              if (inputField.startsWith('"') && inputField.endsWith('"')) {
                inputField = inputField.substring(1, inputField.length - 1);
              }
              
              return {
                Name: item.Name || "",
                LinkedIn: typeof item.LinkedIn === 'string' ? item.LinkedIn : "",
                InputField: inputField
              };
            });
            
            // Store the new data
            apiData = cleanResults;
            
            // If data size changed, validate the current page
            if (oldDataLength !== apiData.length) {
              validateCurrentPage();
            }
            
            // Render the table with current page
            renderTable(currentPage);
            setupPagination();
          } catch (error) {
            console.error("Error processing lead data:", error);
          }
        }
        
        // Handle completion
        if (data.is_final || data.status === 'completed' || data.status === 'error') {
          isProcessing = false;
          eventSource.close();
          eventSource = null;
        }
      } catch (error) {
        console.error("Error parsing SSE message:", error);
        // Don't close the connection on parse errors, just skip this message
      }
    };
    
    eventSource.onerror = (error) => {
      console.error('Error with event stream:', error);
      
      // Don't immediately close on first error - the connection might recover
      const loadingEl = document.getElementById('loading');
      if (loadingEl) loadingEl.textContent = 'Connection issue, retrying...';
      
      // If we have more than 3 consecutive errors, close the connection
      if (++errorCount > 3) {
        isProcessing = false;
        if (eventSource) {
          eventSource.close();
          eventSource = null;
        }
        
        updateStatus('error', 0, campaignIds.length, []);
        
        // Try to get results directly if we have some data already
        if (apiData.length > 0) {
          updateStatus('completed', apiData.length, campaignIds.length, []);
        } else {
          // Try to fetch results directly as a fallback
          fetchAllResults();
        }
      }
    };
  }

  // Fetch all results at once (for when results are already cached)
  async function fetchAllResults() {
    try {
      // Use the match-leads-go endpoint, which includes personalized messages
      const response = await fetch(`${API_BASE_URL}/match-leads-go/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'accept': 'application/json'
        },
        body: JSON.stringify({ 
          campaign_ids: campaignIds
          // Removed pagination parameters as the API doesn't use them
        }),
      });

      if (!response.ok) {
        // Try to get more specific error information if available
        try {
          const errorData = await response.json();
          throw new Error(`API Error: ${errorData.detail || response.status}`);
        } catch {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
      }
      
      const data = await response.json();
      
      // Store old data length for comparison
      const oldDataLength = apiData.length;
      
      // Check if data is an array (the API returns a direct list, not an object with items property)
      if (!Array.isArray(data)) {
        throw new Error('Unexpected API response format: data is not an array');
      }
      
      // Clean the items data to strip quotes from InputField
      const cleanedItems = data.map(item => {
        // Get the input field value and strip quotes if they exist
        let inputField = item.InputField || "";
        if (inputField.startsWith('"') && inputField.endsWith('"')) {
          inputField = inputField.substring(1, inputField.length - 1);
        }
        
        return {
          Name: item.Name || "",
          LinkedIn: typeof item.LinkedIn === 'string' ? item.LinkedIn : "",
          InputField: inputField
        };
      });
      
      // Update the data
      apiData = cleanedItems;
      
      // Validate the current page if data size changed
      if (oldDataLength !== apiData.length) {
        validateCurrentPage();
      }
      
      // Update UI
      updateStatus('completed', apiData.length, campaignIds.length, campaignIds);
      renderTable(currentPage);
      setupPagination();
    } catch (error) {
      console.error('Error fetching all results:', error);
      const loadingEl = document.getElementById('loading');
      if (loadingEl) loadingEl.textContent = 'Error fetching data!';
      const progressEl = document.getElementById('progress');
      if (progressEl) progressEl.textContent = error.message || 'Failed to fetch data';
    }
  }

  // Function to safely escape HTML attribute values
  function escapeHtml(str) {
    if (typeof str !== 'string') return '';
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // Render table with paginated data
  function renderTable(page) {
    const dataContainer = document.getElementById('data-container');
    
    // If no data yet, show message
    if (!apiData.length) {
      dataContainer.innerHTML = '<p>Waiting for data...</p>';
      return;
    }

    // Validate that the current page is within bounds
    validateCurrentPage();
    
    // Calculate the correct start and end indices
    const totalItems = apiData.length;
    const totalPages = Math.ceil(totalItems / itemsPerPage);
    const start = (page - 1) * itemsPerPage;
    const end = Math.min(start + itemsPerPage, totalItems);
    
    const paginatedData = apiData.slice(start, end);
    
    const tableHTML = `
      <table>
        <tr>
          <th>Name</th>
          <th>LinkedIn Profile</th>
          <th>Input Field</th>
        </tr>
        ${paginatedData.map(item => `
        <tr>
          <td>${escapeHtml(item.Name)}</td>
          <td>
            <a href="${escapeHtml(item.LinkedIn)}" target="_blank">Profile</a>
            <button class="copy-btn" data-url="${escapeHtml(item.LinkedIn)}">Copy URL</button>
          </td>
          <td>
            <input type="text" class="input-field" value="${escapeHtml(item.InputField)}">
            <button class="copy-btn">Copy</button>
          </td>
        </tr>
        `).join('')}
      </table>
    `;

    dataContainer.innerHTML = tableHTML;
    addCopyListeners();
    updatePageInfo();
  }

  // Add copy functionality
  function addCopyListeners() {
    document.querySelectorAll('.copy-btn').forEach(button => {
      button.addEventListener('click', (e) => {
        const text = button.dataset.url || 
          e.target.closest('tr').querySelector('.input-field').value;
        
        navigator.clipboard.writeText(text).then(() => {
          button.textContent = 'Copied!';
          setTimeout(() => {
            button.textContent = button.dataset.url ? 'Copy URL' : 'Copy';
          }, 1500);
        });
      });
    });
  }

  // Set up pagination - track if it's been initialized
  let paginationInitialized = false;
  
  function setupPagination() {
    // If pagination is already set up, don't add more listeners
    if (paginationInitialized) {
      // Just update the page information and make sure current page is valid
      validateCurrentPage();
      updatePageInfo();
      return;
    }
    
    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');
    
    if (!prevBtn || !nextBtn) {
      return;
    }
    
    // Add event listeners only once
    prevBtn.addEventListener('click', () => {
      if (currentPage > 1) {
        currentPage--;
        renderTable(currentPage);
      }
    });

    nextBtn.addEventListener('click', () => {
      if (currentPage < Math.ceil(apiData.length / itemsPerPage)) {
        currentPage++;
        renderTable(currentPage);
      }
    });
    
    // Mark pagination as initialized
    paginationInitialized = true;
  }
  
  // Validate that the current page is within bounds
  function validateCurrentPage() {
    const totalPages = Math.ceil(apiData.length / itemsPerPage);
    
    // If current page is greater than total pages, adjust it
    if (currentPage > totalPages && totalPages > 0) {
      currentPage = totalPages;
    }
    
    // Make sure page is at least 1
    if (currentPage < 1) {
      currentPage = 1;
    }
  }

  // Update page information
  function updatePageInfo() {
    const totalItems = apiData.length;
    const totalPages = Math.ceil(totalItems / itemsPerPage);
    
    // Enable/disable buttons based on current page
    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');
    
    if (prevBtn) prevBtn.disabled = currentPage <= 1;
    if (nextBtn) nextBtn.disabled = currentPage >= totalPages;
    
    // Update the page info text
    const pageInfo = document.getElementById('pageInfo');
    if (pageInfo) {
      pageInfo.textContent = `Page ${currentPage} of ${totalPages || 1}`;
    }
  }

  // Initialize the extension by starting to process data
  startProcessing();
});