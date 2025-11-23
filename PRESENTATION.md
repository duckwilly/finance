# Presentation

We moeten nog even kijken hoe we dit willen verdelen. Check zelf maar welke onderwerpen jouw voorkeur hebben en dan doe ik de rest. 

## Topics

- Overview of the project (A)
    - Simulated bank data for individuals and companies, including transactions and stock holdings
    - Admin dashboard for browsing and searching individual and company data, big picture view of all the data
    - Individuals can log in and view their own data and data for their company
- Tech stack (A)
    - Frontend: HTML, CSS, JavaScript, HTMX, Chart.js 
    - Backend: Python, SQLAlchemy, FastAPI, Jinja2, Pydantic, PyJWT
    - Database: MariaDB on Docker
- Database design (M)
    - Nature of financial data
    - Structure of the database
    - Relationships between tables
    - Goals and design decisions
- Features/demo (M)
    - Authentication and authorization
    - Admin dashboard
        - Big picture view of the database
        - Views of companies and individuals, transactions and holdings
    - Company dashboard
    - Individual dashboard
    - Insights using AI chatbot
        - Use an example of a company trending down or up with their profits, get AI to generate a chart and table showing the trend
    - Show log (A)
- AI chatbot features (A)
    - Prompt assembly
    - JSON response
    - Python database querying
    - Dashboard charts generation with httpx

