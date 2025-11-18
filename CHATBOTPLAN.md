# Chatbot Plan

## Workflow

- User sends a message to the chatbot
- Prompt is generated. Prompt includes 
    - Header with info about the app
    - Context about the current page (e.g. "You are on the dashboard for user John Doe")
    - Context about the current user (e.g. "You are logged in as John Doe")
    - Structure of the database
    - Allowed queries and their limitations
    - JSON response format
    - Example of a valid query and its response
    - User query and response history
- Response by AI features
    - Response message to user for in chat window
    - keywords for sqlalchemy queries that will dynamically generate charts and tables
- Keywords for sqlalchemy queries are loaded into a python script that queries the database
- The current page is updated with the new data (chat window is updated with the response message and charts and tables are rendered)

## Frontend

- Each dashboard will have dashboard cards above, which open different views of the dashboard (currently implemented in the admin dashboard). The bottom central view can be populated by these cards but also dynamically by the chatbot.
- Chat button opens a window that takes up 1/3 of the screen width underneath the dashboard cards, with the dashboard taking up the remaining 2/3
- Chat window should be collapsible and expandable
- Chat should be fully visible in the visible height of the screen, i.e. chatbox should be visible without scrolling down

## Backend 

- The LLM response will be a JSON object that contains a plaintext response to the user and keywords for sqlalchemy queries that will dynamically generate charts and tables. It should generate between 0 and 3 charts and/or tables. 

### End-to-End Workflow

- **Prompt Assembly**:  
  Create a prompt builder that injects the following, before each LLM call, so responses are always page-aware:
    - Application header
    - Current page and user context
    - Database schema
    - Allowed queries and limitations
    - JSON response schema for output
    - Example conversation exchange
    - Conversation (chat) history

- **LLM Response Contract**:  
  Ensure every LLM response returns a JSON object containing:
    - `(a)` A plaintext reply for the chat window
    - `(b)` Zero to three chart/table generation keywords for downstream SQLAlchemy queries

- **Post-Processor & Database Querying**:  
  - Extract the generated SQLAlchemy keywords from the LLM’s JSON response
  - Pass these to a Python query runner
  - Return structured data ready for chart/table rendering on the current dashboard page

- **Dashboard/UI Integration**:  
  - Update the chatbot response handler to refresh the dashboard section with new visualizations alongside the chat reply, once data retrieval is complete

---

### Frontend Integration

- Preserve dashboard cards as the main navigation method
- Permit the chatbot to populate the bottom central dashboard view dynamically with generated charts/tables (when present)
- Implement a chat toggle:
    - Opens to approximately 1/3 screen width beneath the cards
    - The dashboard uses the remaining ~2/3
    - Chat panel should be collapsible/expandable and always fully visible within viewport height (chatbox always visible without vertical scrolling)
- Render chatbot replies plus any returned charts/tables in the dashboard area
- If no visualizations are requested, maintain the existing card-driven view

---

### Backend Integration

- Define and validate the JSON schema for each LLM response (plaintext reply + 0–3 visualization descriptors)  
- Handle and log invalid payloads gracefully
- Map each visualization descriptor to a parameterized SQLAlchemy query, enforcing adherence to the allowed queries and schema detailed in the prompt
- Implement a query runner:
    - Executes generated keywords
    - Returns datasets for charts/tables
    - Surfaces errors in the chat reply if something goes wrong
- Integrate the runner into the chatbot service so new data both updates the chat thread and triggers dashboard refreshes

---

### Operational Considerations

- Add monitoring/logging for:
    - Prompt assembly
    - LLM responses
    - Query execution  
  (following project logging guidelines)
- Keep all documentation and README files up to date as the JSON contract and UI entry points are finalized

