/**
 * AI Chatbot Frontend Logic
 * Handles HTMX responses, chart rendering, and UI updates
 */

// Global state
let conversationHistory = [];
let currentChart = null;

const getQuestionInput = () => document.getElementById('question-input');

const AUTOSIZE_DEFAULT_MIN_HEIGHT = 64;
const AUTOSIZE_DEFAULT_MAX_HEIGHT = 240;

const resizeTextarea = (element) => {
    if (!element) return;
    const minHeight = parseInt(element.dataset.minHeight || AUTOSIZE_DEFAULT_MIN_HEIGHT, 10);
    const maxHeight = parseInt(element.dataset.maxHeight || AUTOSIZE_DEFAULT_MAX_HEIGHT, 10);
    element.style.height = 'auto';
    const desired = Math.min(Math.max(element.scrollHeight, minHeight), maxHeight);
    element.style.height = `${desired}px`;
    element.style.overflowY = element.scrollHeight > desired ? 'auto' : 'hidden';
};

const bindTextareaAutosize = (element) => {
    if (!element || element.tagName !== 'TEXTAREA' || element.dataset.autosizeBound === 'true') {
        return;
    }
    element.dataset.autosizeBound = 'true';
    resizeTextarea(element);
    element.addEventListener('input', () => resizeTextarea(element));
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initializeChatbot();
});

function initializeChatbot() {
    // Sync model selector with hidden form input
    const modelSelect = document.getElementById('model-select');
    const modelInput = document.getElementById('model-input');

    if (modelSelect && modelInput) {
        const syncModel = () => {
            modelInput.value = modelSelect.value;
        };
        modelSelect.addEventListener('change', syncModel);
        syncModel();
    }

    // Handle form submission
    const chatForm = document.querySelector('[data-chatbot-form]');
    const questionInput = getQuestionInput();
    if (!chatForm || !questionInput) {
        bindChatSuggestions();
        return;
    }

    bindTextareaAutosize(questionInput);

    chatForm.addEventListener('submit', function() {
        const question = questionInput.value.trim();

        if (question) {
            addUserMessage(question);
            // Clear input will happen after HTMX response
        }
    });

    // HTMX afterRequest event to clear input
    document.body.addEventListener('htmx:afterRequest', function(event) {
        const sourceEl = event.detail?.requestConfig?.elt;
        if (!sourceEl || !sourceEl.hasAttribute('data-chatbot-form')) {
            return;
        }
        const questionInput = getQuestionInput();
        if (!questionInput) return;
        questionInput.value = '';
        resizeTextarea(questionInput);
        questionInput.focus();
    });

    bindChatSuggestions();
}

// Global handler for HTMX responses
window.handleChatbotResponse = function(responseData) {
    const { response, chart_config, chart_title, table_data, mode, visualizations } = responseData;

    // Add assistant message to chat
    addAssistantMessage(response);

    // Update conversation history
    const questionInput = getQuestionInput();
    const lastQuestion = questionInput?.getAttribute('data-last-question') || '';
    if (lastQuestion) {
        conversationHistory.push({
            role: 'user',
            content: lastQuestion
        });
    }
    conversationHistory.push({
        role: 'assistant',
        content: response
    });

    // Keep only last 6 messages
    if (conversationHistory.length > 6) {
        conversationHistory = conversationHistory.slice(-6);
    }

    const visualizationList = visualizations || [];

    if (mode === 'visualization' && visualizationList.length > 0) {
        renderVisualizationStack(visualizationList);
    } else if (mode === 'visualization' && (chart_config || chart_title)) {
        if (chart_config) {
            renderChart(chart_config, chart_title);
        } else {
            renderChartError('Unable to render chart');
        }

        if (table_data && table_data.length > 0) {
            renderTable(table_data);
        }
    } else {
        hideLegacyVisuals();
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
    if (!chatMessages) return;

    // Remove welcome message if it exists
    const welcomeMessage = chatMessages.querySelector('.welcome-message');
    if (welcomeMessage) {
        welcomeMessage.remove();
    }

    // Store question for history
    const questionInput = getQuestionInput();
    if (questionInput) {
        questionInput.setAttribute('data-last-question', message);
    }

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
    if (!chatMessages) return;

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

function renderChartError(message) {
    const stack = document.getElementById('chat-visualizations');
    if (stack) {
        stack.innerHTML = '';
        const card = document.createElement('div');
        card.className = 'chat-visualization__error';
        card.textContent = message;
        stack.appendChild(card);
    } else {
        addErrorMessage(message);
    }
}

function renderChart(chartConfig, title, targetId = 'chart-container') {
    const chartContainer = document.getElementById(targetId);
    if (!chartContainer) return;

    const chartCanvas = chartContainer.querySelector('canvas') || document.getElementById('chart-canvas');
    const chartTitle = chartContainer.querySelector('.chart-title') || document.getElementById('chart-title');

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
    const hydratedConfig = hydrateChartConfig(chartConfig);
    currentChart = new Chart(ctx, hydratedConfig);

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

    const elements = buildTableElements(data, { withCurrency: true });
    tableHead.appendChild(elements.head);
    tableBody.appendChild(elements.body);
}

function formatCurrency(value) {
    return '€' + value.toLocaleString('de-DE', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

function buildTableElements(data, { withCurrency = false } = {}) {
    const columns = Object.keys(data[0]);
    const currencyColumns = new Set(
        columns.filter((col) => {
            const lowered = col.toLowerCase();
            return /(total|amount|value|income|expense|spend|balance|market|unrealized)/.test(lowered);
        })
    );

    const head = document.createElement('thead');
    const headerRow = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col.replace(/_/g, ' ').toUpperCase();
        headerRow.appendChild(th);
    });
    head.appendChild(headerRow);

    const body = document.createElement('tbody');
    data.forEach(row => {
        const tr = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            const value = row[col];

            if (withCurrency && currencyColumns.has(col) && typeof value === 'number' && isFinite(value)) {
                td.className = 'currency';
                td.textContent = formatCurrency(value);
            } else {
                td.textContent = value ?? '-';
            }

            tr.appendChild(td);
        });
        body.appendChild(tr);
    });

    return { head, body };
}

function renderVisualizationStack(visualizations) {
    const stack = document.getElementById('chat-visualizations');
    if ((!stack || !visualizations.length)) {
        const primary = visualizations[0];
        if (!primary) return;
        if (primary.chart_error) {
            renderChartError(primary.chart_error);
        } else if (primary.chart_config) {
            renderChart(primary.chart_config, primary.chart_title || primary.title);
        }
        if (primary?.table_data?.length) {
            renderTable(primary.table_data);
        }
        return;
    }

    stack.innerHTML = '';

    visualizations.forEach((viz, index) => {
        const card = document.createElement('div');
        card.className = 'chat-visualization';

        if (viz.chart_title || viz.title) {
            const titleEl = document.createElement('h4');
            titleEl.className = 'chat-visualization__title';
            titleEl.textContent = viz.chart_title || viz.title;
            card.appendChild(titleEl);
        }

        if (viz.chart_error) {
            const errorEl = document.createElement('div');
            errorEl.className = 'chat-visualization__error';
            errorEl.textContent = viz.chart_error;
            card.appendChild(errorEl);
        }

        if (viz.chart_config) {
            const canvas = document.createElement('canvas');
            canvas.id = `chat-viz-${index}`;
            card.appendChild(canvas);

            const hydrated = hydrateChartConfig(viz.chart_config);
            new Chart(canvas.getContext('2d'), hydrated);
        }

        if (viz.table_data && viz.table_data.length) {
            const tableWrapper = document.createElement('div');
            tableWrapper.className = 'chat-visualization__table';
            const table = document.createElement('table');
            const elements = buildTableElements(viz.table_data, { withCurrency: true });
            table.appendChild(elements.head);
            table.appendChild(elements.body);
            tableWrapper.appendChild(table);
            card.appendChild(tableWrapper);
        }

        stack.appendChild(card);
    });
}

function hydrateChartConfig(config) {
    if (!config) return null;
    const parsed = typeof config === 'string' ? JSON.parse(config) : config;
    const clone = typeof structuredClone === 'function' ? structuredClone(parsed) : JSON.parse(JSON.stringify(parsed));

    const revive = (value) => {
        if (value === '##CURRENCY_CALLBACK##') {
            return (val) => `€${Number(val).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        }
        if (value === '##CURRENCY_TOOLTIP##') {
            return (context) => {
                const label = context.dataset?.label ? `${context.dataset.label}: ` : '';
                const amount = context.parsed?.y ?? context.raw;
                return `${label}€${Number(amount).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
            };
        }
        if (Array.isArray(value)) return value.map(revive);
        if (value && typeof value === 'object') {
            Object.keys(value).forEach((key) => {
                value[key] = revive(value[key]);
            });
        }
        return value;
    };

    return revive(clone);
}

function hideLegacyVisuals() {
    const chartContainer = document.getElementById('chart-container');
    const tableContainer = document.getElementById('table-container');
    const stack = document.getElementById('chat-visualizations');
    if (chartContainer) chartContainer.style.display = 'none';
    if (tableContainer) tableContainer.style.display = 'none';
    if (stack) stack.innerHTML = '';
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function scrollToBottom() {
    const chatMessages = document.getElementById('chat-messages');
    if (chatMessages) {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }
}

function bindChatSuggestions() {
    const questionInput = getQuestionInput();
    if (!questionInput) return;

    document.querySelectorAll('[data-chat-suggestion]').forEach((button) => {
        button.addEventListener('click', () => {
            const suggestion = button.dataset.chatSuggestion || '';
            questionInput.value = suggestion;
            questionInput.focus();
        });
    });
}

// Export for external use
window.chatbot = {
    getConversationHistory: () => conversationHistory,
    clearHistory: () => {
        conversationHistory = [];
    },
    resetUI: () => {
        const messages = document.getElementById('chat-messages');
        if (messages) messages.innerHTML = '';
        const chartContainer = document.getElementById('chart-container');
        if (chartContainer) chartContainer.style.display = 'none';
        const tableContainer = document.getElementById('table-container');
        if (tableContainer) tableContainer.style.display = 'none';
        const stack = document.getElementById('chat-visualizations');
        if (stack) stack.innerHTML = '';
        if (currentChart) {
            currentChart.destroy();
            currentChart = null;
        }
    }
};
