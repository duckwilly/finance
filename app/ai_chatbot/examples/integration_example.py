"""
Complete Integration Example for AI Chatbot Package
This example shows how to integrate the chatbot into a FastAPI application
"""

from fastapi import FastAPI, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Dict, Any
import os

# Import chatbot package
from app.ai_chatbot import (
    router as chatbot_router,
    configure_dependencies,
    configure_templates,
    chatbot_config,
    llm_config,
)

# ============================================================================
# 1. DATABASE SETUP
# ============================================================================

# Your database URL (example for MariaDB/MySQL)
DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://user:pass@localhost/finance_db")

# Create engine and session
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Session:
    """Database dependency - yields a database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================================
# 2. AUTHENTICATION SETUP
# ============================================================================

def get_current_user() -> Dict[str, Any]:
    """
    User authentication dependency

    Returns user context with:
    - role: 'person', 'company', or 'admin'
    - person_id: ID for person role
    - company_id: ID for company role
    - username: Display name

    Replace this with your actual authentication logic!
    """

    # EXAMPLE 1: Simple hardcoded user (for testing)
    return {
        "role": "person",
        "person_id": 1,
        "company_id": None,
        "username": "John Doe"
    }

    # EXAMPLE 2: JWT token authentication
    """
    from fastapi import Header
    from jose import jwt, JWTError

    def get_current_user(authorization: str = Header(None)):
        if not authorization:
            raise HTTPException(401, "Not authenticated")

        try:
            token = authorization.replace("Bearer ", "")
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])

            return {
                "role": payload.get("role"),
                "person_id": payload.get("person_id"),
                "company_id": payload.get("company_id"),
                "username": payload.get("username")
            }
        except JWTError:
            raise HTTPException(401, "Invalid token")
    """

    # EXAMPLE 3: Session-based authentication
    """
    from fastapi import Request

    def get_current_user(request: Request):
        user_id = request.session.get("user_id")
        if not user_id:
            raise HTTPException(401, "Not authenticated")

        # Fetch user from database
        db = next(get_db())
        user = db.query(User).filter(User.id == user_id).first()

        return {
            "role": user.role,
            "person_id": user.person_id,
            "company_id": user.company_id,
            "username": f"{user.first_name} {user.last_name}"
        }
    """


# ============================================================================
# 3. CUSTOM DATABASE SCHEMA (Optional)
# ============================================================================

# Define your database schema for the LLM
# This helps the LLM generate accurate SQL queries
CUSTOM_DATABASE_SCHEMA = """
Financial Database Schema:

Tables:
1. transactions
   - id (INT, Primary Key)
   - account_id (INT, Foreign Key -> accounts.id)
   - category_id (INT, Foreign Key -> transaction_categories.id)
   - posted_at (DATETIME)
   - transaction_date (DATE)
   - amount (DECIMAL 15,2)
   - direction (ENUM: 'CREDIT', 'DEBIT')
   - channel (ENUM: 'SEPA', 'CARD', 'INTERNAL', 'WIRE', 'CASH')
   - description (TEXT)

2. transaction_categories
   - id (INT, Primary Key)
   - section_name (VARCHAR: 'income', 'expense', 'transfer')
   - category_name (VARCHAR: 'rent', 'salary', 'groceries', etc.)

3. accounts
   - id (INT, Primary Key)
   - account_type (ENUM: 'CHK', 'SAV', 'OP', 'CC', 'LOAN')
   - person_id (INT, Foreign Key -> persons.id)
   - company_id (INT, Foreign Key -> companies.id)

4. persons
   - id (INT, Primary Key)
   - first_name (VARCHAR)
   - last_name (VARCHAR)

5. companies
   - id (INT, Primary Key)
   - name (VARCHAR)
   - industry_category (VARCHAR)

Important Relationships:
- Transactions are linked to accounts via account_id
- Accounts belong to either a person or a company
- Transactions are categorized via category_id
- Use table aliases: t (transactions), tc (transaction_categories), a (accounts), p (persons), c (companies)
"""


# ============================================================================
# 4. FASTAPI APPLICATION SETUP
# ============================================================================

app = FastAPI(
    title="Financial Dashboard with AI Chatbot",
    description="Complete example of AI chatbot integration",
    version="1.0.0"
)

# Setup templates
# IMPORTANT: Create a 'templates' directory with 'ai_chatbot' subdirectory
# Copy the frontend HTML files to templates/ai_chatbot/
templates = Jinja2Templates(directory="templates")

# Setup static files
# IMPORTANT: Create a 'static' directory with 'ai_chatbot' subdirectory
# Copy the CSS and JS files to static/ai_chatbot/
app.mount("/static", StaticFiles(directory="static"), name="static")


# ============================================================================
# 5. CONFIGURE CHATBOT
# ============================================================================

# Configure templates for chatbot
configure_templates(templates)

# Configure dependencies
configure_dependencies(
    get_db=get_db,
    get_user=get_current_user,
    database_schema=CUSTOM_DATABASE_SCHEMA  # Optional: use custom schema
)

# Optional: Customize chatbot configuration
chatbot_config.chart_color_palette = [
    "#E87722",  # Primary brand color
    "#F7A54A",
    "#FFC845",
    "#D04A02",
    "#F0AB9E"
]
chatbot_config.max_conversation_history = 6

# Optional: Customize LLM configuration
llm_config.claude_model = "claude-haiku-4-5-20251001"
llm_config.openai_model = "gpt-4o-mini"


# ============================================================================
# 6. INCLUDE CHATBOT ROUTER
# ============================================================================

# Include chatbot routes at /ai-chatbot/*
app.include_router(chatbot_router)

# Alternative: Mount at different path
# from fastapi import APIRouter
# custom_router = APIRouter(prefix="/chat")
# custom_router.include_router(chatbot_router)
# app.include_router(custom_router)


# ============================================================================
# 7. EXAMPLE: YOUR OTHER ROUTES
# ============================================================================

@app.get("/")
async def home():
    """Your main dashboard page"""
    return {
        "message": "Welcome to Financial Dashboard",
        "chatbot_url": "/ai-chatbot/"
    }


@app.get("/dashboard")
async def dashboard(user: dict = Depends(get_current_user)):
    """Protected dashboard route"""
    return {
        "message": f"Welcome {user['username']}",
        "role": user["role"]
    }


# ============================================================================
# 8. EXAMPLE: PROGRAMMATIC CHATBOT USAGE
# ============================================================================

from app.ai_chatbot import FinancialChatbot

@app.get("/api/quick-insights")
async def get_quick_insights(
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    """
    Example: Use chatbot programmatically to generate insights
    """
    chatbot = FinancialChatbot(database_schema=CUSTOM_DATABASE_SCHEMA)

    # Generate financial summary
    summary = chatbot.get_financial_summary(user, db)

    # Process a predefined question
    result = await chatbot.process_query(
        question="Show me my top 5 expense categories",
        provider_name="claude-haiku-4-5-20251001",
        user_context=user,
        db_session=db,
        response_mode="visualization"
    )

    return {
        "summary": summary,
        "chart": result["chart_config"],
        "data": result["table_data"]
    }


# ============================================================================
# 9. RUN THE APPLICATION
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    # Development server
    uvicorn.run(
        "integration_example:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )

    # Production server
    # uvicorn.run(
    #     "integration_example:app",
    #     host="0.0.0.0",
    #     port=8000,
    #     workers=4
    # )


# ============================================================================
# 10. SETUP CHECKLIST
# ============================================================================

"""
INTEGRATION CHECKLIST:

1. Install dependencies:
   pip install -r requirements.txt

2. Setup database:
   - Update DATABASE_URL
   - Ensure tables exist
   - Create indexes for performance

3. Configure LLM providers:
   - Get a Claude API key from Anthropic (Haiku 4.5)
   - Get an OpenAI API key with access to GPT 4o Mini (or another supported Chat Completions model)
   - Confirm billing/quotas for both providers

4. Create directory structure:
   your_project/
   ├── templates/
   │   └── ai_chatbot/
   │       ├── chatbot.html
   │       ├── response.html
   │       └── error_message.html
   ├── static/
   │   └── ai_chatbot/
   │       ├── chatbot.css
   │       └── chatbot.js
   └── integration_example.py

5. Configure environment variables (.env):
   DATABASE_URL=mysql+pymysql://user:pass@localhost/dbname
   CLAUDE_API_KEY=sk-...
   OPENAI_API_KEY=sk-...

6. Implement get_current_user():
   - Replace example with your auth logic
   - Return proper user context dict

7. Test the integration:
   - Start server: uvicorn integration_example:app --reload
   - Visit: http://localhost:8000/ai-chatbot/
   - Test queries: "Show me my expenses by category"

8. Deploy to production:
   - Use proper WSGI server (uvicorn with workers)
   - Setup HTTPS
   - Configure CORS if needed
   - Setup monitoring/logging
"""


# ============================================================================
# 11. ADVANCED EXAMPLES
# ============================================================================

# Example: Custom LLM Provider
"""
from app.ai_chatbot.llm_providers import LLMProvider

class CustomLLMProvider(LLMProvider):
    async def query(self, system_prompt, user_prompt, conversation_history=None, json_mode=True):
        # Your custom LLM integration
        response = await your_llm_api.call(...)
        return {
            "content": response,
            "model": "custom-model",
            "provider": "custom"
        }

# Register custom provider
from app.ai_chatbot.llm_providers import LLMProviderFactory
LLMProviderFactory.providers["custom"] = CustomLLMProvider
"""

# Example: Custom Quick Template
"""
from app.ai_chatbot import QuickTemplateManager

template_mgr = QuickTemplateManager()
template_mgr.templates["profit_loss"] = {
    "keywords": ["profit and loss", "p&l", "income statement"],
    "sql": '''
        SELECT
            DATE_FORMAT(t.transaction_date, '%Y-%m') as period,
            SUM(CASE WHEN tc.section_name = 'income' THEN t.amount ELSE 0 END) as revenue,
            SUM(CASE WHEN tc.section_name = 'expense' THEN t.amount ELSE 0 END) as expenses,
            SUM(CASE WHEN tc.section_name = 'income' THEN t.amount ELSE -t.amount END) as net_profit
        FROM transactions t
        JOIN transaction_categories tc ON t.category_id = tc.id
        JOIN accounts a ON t.account_id = a.id
        WHERE 1=1 {filter}
        GROUP BY period
        ORDER BY period DESC
        LIMIT 12
    ''',
    "explanation": "Monthly profit and loss statement"
}
"""

# Example: Webhook Integration
"""
@app.post("/webhooks/chatbot-query")
async def chatbot_webhook(
    question: str,
    user_id: int,
    db: Session = Depends(get_db)
):
    # Process chatbot query from external system (Slack, Teams, etc.)
    user_context = {
        "role": "person",
        "person_id": user_id,
        "company_id": None,
        "username": f"User {user_id}"
    }

    chatbot = FinancialChatbot()
    result = await chatbot.process_query(
        question=question,
        provider_name="claude-haiku-4-5-20251001",
        user_context=user_context,
        db_session=db
    )

    return {
        "answer": result["response"],
        "has_chart": result["chart_config"] is not None
    }
"""
