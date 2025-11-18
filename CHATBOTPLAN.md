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

- The JSON response should be formatted as follows: