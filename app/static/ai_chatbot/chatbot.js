/**
 * AI Chatbot Frontend Logic
 * Handles HTMX responses, chart rendering, and UI updates
 */

// Global state
let conversationHistory = [];
let currentChart = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initializeChatbot();
});

function initializeChatbot() {
    // Sync model selector with hidden form input
    const modelSelect = document.getElementById('model-select');
    const modelInput = document.getElementById('model-input');

    modelSelect.addEventListener('change', function() {
        modelInput.value = modelSelect.value;
    });

    // Set initial value
    modelInput.value = modelSelect.value;

    // Handle form submission
    const chatForm = document.getElementById('chat-form');
    chatForm.addEventListener('submit', function(e) {
        // Don't prevent default - HTMX handles it
        // Just add user message to UI
        const questionInput = document.getElementById('question-input');
        const question = questionInput.value.trim();

        if (question) {
            addUserMessage(question);
            // Clear input will happen after HTMX response
        }
    });

    // HTMX afterRequest event to clear input
    document.body.addEventListener('htmx:afterRequest', function(event) {
        const questionInput = document.getElementById('question-input');
        questionInput.value = '';
        questionInput.focus();
    });
}

// Global handler for HTMX responses
window.handleChatbotResponse = function(responseData) {
    const { response, chart_config, chart_title, table_data, mode } = responseData;

    // Add assistant message to chat
    addAssistantMessage(response);

    // Update conversation history
    conversationHistory.push({
        role: 'user',
        content: document.getElementById('question-input').getAttribute('data-last-question') || ''
    });
    conversationHistory.push({
        role: 'assistant',
        content: response
    });

    // Keep only last 6 messages
    if (conversationHistory.length > 6) {
        conversationHistory = conversationHistory.slice(-6);
    }

    // Handle visualization mode
    if (mode === 'visualization' && chart_config) {
        renderChart(chart_config, chart_title);

        if (table_data && table_data.length > 0) {
            renderTable(table_data);
        }
    } else {
        // Hide chart and table for conversational mode
        document.getElementById('chart-container').style.display = 'none';
        document.getElementById('table-container').style.display = 'none';
    }

    // Scroll to bottom of messages
    scrollToBottom();
};

// Global error handler
window.handleChatbotError = function(error) {
    addErrorMessage(error);
    scrollToBottom();
};

function addUserMessage(message) {
    const chatMessages = document.getElementById('chat-messages');

    // Remove welcome message if it exists
    const welcomeMessage = chatMessages.querySelector('.welcome-message');
    if (welcomeMessage) {
        welcomeMessage.remove();
    }

    // Store question for history
    document.getElementById('question-input').setAttribute('data-last-question', message);

    const messageDiv = document.createElement('div');
    messageDiv.className = 'message user';
    messageDiv.innerHTML = `
        <div class="message-content">${escapeHtml(message)}</div>
        <div class="message-meta">${new Date().toLocaleTimeString()}</div>
    `;

    chatMessages.appendChild(messageDiv);
}

function addAssistantMessage(message) {
    const chatMessages = document.getElementById('chat-messages');

    const messageDiv = document.createElement('div');
    messageDiv.className = 'message assistant';
    messageDiv.innerHTML = `
        <div class="message-content">${formatPlainText(message)}</div>
        <div class="message-meta">AI Assistant • ${new Date().toLocaleTimeString()}</div>
    `;

    chatMessages.appendChild(messageDiv);
}

function formatPlainText(text) {
    // Escape HTML to prevent XSS
    let html = escapeHtml(text);

    // Convert line breaks to HTML
    html = html.replace(/\n/g, '<br>');

    return html;
}

function addErrorMessage(error) {
    const chatMessages = document.getElementById('chat-messages');

    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.textContent = `Error: ${error}`;

    chatMessages.appendChild(errorDiv);
}

function renderChart(chartConfig, title) {
    const chartContainer = document.getElementById('chart-container');
    const chartCanvas = document.getElementById('chart-canvas');
    const chartTitle = document.getElementById('chart-title');

    // Show container
    chartContainer.style.display = 'block';

    // Set title
    if (title) {
        chartTitle.textContent = title;
    }

    // Destroy previous chart if exists
    if (currentChart) {
        currentChart.destroy();
    }

    // Create new chart
    const ctx = chartCanvas.getContext('2d');
    currentChart = new Chart(ctx, chartConfig);

    // Scroll to chart
    chartContainer.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function renderTable(data) {
    const tableContainer = document.getElementById('table-container');
    const tableHead = document.getElementById('table-head');
    const tableBody = document.getElementById('table-body');

    // Show container
    tableContainer.style.display = 'block';

    // Clear previous content
    tableHead.innerHTML = '';
    tableBody.innerHTML = '';

    if (data.length === 0) {
        tableContainer.style.display = 'none';
        return;
    }

    // Get column names from first row
    const columns = Object.keys(data[0]);

    // Create header row
    const headerRow = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col.replace(/_/g, ' ').toUpperCase();
        headerRow.appendChild(th);
    });
    tableHead.appendChild(headerRow);

    // Create data rows
    data.forEach(row => {
        const tr = document.createElement('tr');

        columns.forEach(col => {
            const td = document.createElement('td');
            const value = row[col];

            // Check if value is a number (for currency formatting)
            if (typeof value === 'number' && isFinite(value)) {
                td.className = 'currency';
                td.textContent = formatCurrency(value);
            } else {
                td.textContent = value || '-';
            }

            tr.appendChild(td);
        });

        tableBody.appendChild(tr);
    });
}

function formatCurrency(value) {
    return '€' + value.toLocaleString('de-DE', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function scrollToBottom() {
    const chatMessages = document.getElementById('chat-messages');
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

// Export for external use
window.chatbot = {
    getConversationHistory: () => conversationHistory,
    clearHistory: () => {
        conversationHistory = [];
    },
    resetUI: () => {
        document.getElementById('chat-messages').innerHTML = '';
        document.getElementById('chart-container').style.display = 'none';
        document.getElementById('table-container').style.display = 'none';
        if (currentChart) {
            currentChart.destroy();
            currentChart = null;
        }
    }
};
