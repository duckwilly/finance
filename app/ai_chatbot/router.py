"""
FastAPI Router for AI Chatbot
Ready-to-use API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional, List, Literal
from sqlalchemy.orm import Session
import logging

from .chatbot_core import FinancialChatbot
from .chart_generator import ChartGenerator

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/chatbot", tags=["AI Chatbot"])

# Templates (will be configured by the integrating application)
templates = None


def configure_templates(templates_instance: Jinja2Templates):
    """Configure Jinja2 templates"""
    global templates
    templates = templates_instance


# Pydantic models for request/response
class ChatbotQueryRequest(BaseModel):
    """Request model for chatbot query"""
    question: str
    model: str = "ollama:llama3:latest"
    conversation_history: Optional[List[dict]] = None
    response_mode: Optional[Literal["visualization", "conversational"]] = None
    page_context: Optional[str] = None


class ChatbotQueryResponse(BaseModel):
    """Response model for chatbot query"""
    response: str
    chart_config: Optional[dict] = None
    chart_title: Optional[str] = None
    table_data: Optional[List[dict]] = None
    sql_query: Optional[str] = None
    mode: str
    visualizations: Optional[List[dict]] = None


# Dependency injection placeholders (to be configured by integrating app)
_get_db_session = None
_get_current_user = None
_chatbot_instance = None


def configure_dependencies(
    get_db: callable,
    get_user: callable,
    database_schema: Optional[str] = None
):
    """
    Configure dependencies for the chatbot router

    Args:
        get_db: Dependency function that yields database session
        get_user: Dependency function that returns current user context
        database_schema: Optional custom database schema description
    """
    global _get_db_session, _get_current_user, _chatbot_instance

    _get_db_session = get_db
    _get_current_user = get_user
    _chatbot_instance = FinancialChatbot(database_schema)


def get_db_session():
    """Get database session dependency"""
    if _get_db_session is None:
        raise RuntimeError("Database dependency not configured. Call configure_dependencies() first.")
    # The configured function is a generator, so we need to yield from it
    yield from _get_db_session()


def get_current_user(request: Request):
    """Get current user dependency"""
    if _get_current_user is None:
        raise RuntimeError("User dependency not configured. Call configure_dependencies() first.")
    # Call the configured user function with the request
    return _get_current_user(request)


def get_chatbot():
    """Get chatbot instance"""
    if _chatbot_instance is None:
        raise RuntimeError("Chatbot not initialized. Call configure_dependencies() first.")
    return _chatbot_instance


# Routes
@router.get("/", response_class=HTMLResponse)
async def chatbot_page(request: Request):
    """Render chatbot interface page"""
    if templates is None:
        raise HTTPException(500, "Templates not configured")

    return templates.TemplateResponse(
        "ai_chatbot/chatbot.html",
        {"request": request}
    )


@router.post("/query", response_model=ChatbotQueryResponse)
async def chatbot_query(
    request: ChatbotQueryRequest,
    http_request: Request,
    db: Session = Depends(get_db_session),
    user_context: dict = Depends(get_current_user)
):
    """
    Process chatbot query

    Request body:
    - question: Natural language question
    - model: LLM provider (e.g., 'ollama:llama3:latest', 'claude', 'chatgpt')
    - conversation_history: Optional list of previous messages
    - response_mode: Optional 'visualization' or 'conversational'

    Returns:
    - response: Text response
    - chart_config: Chart.js config (if visualization mode)
    - chart_title: Chart title (if visualization mode)
    - table_data: Query results (if visualization mode)
    - sql_query: Generated SQL (if visualization mode)
    - mode: Response mode used
    """
    try:
        chatbot = get_chatbot()

        # Get financial summary for RAG context
        financial_summary = chatbot.get_financial_summary(user_context, db)

        # Process query
        result = await chatbot.process_query(
            question=request.question,
            provider_name=request.model,
            user_context=user_context,
            db_session=db,
            conversation_history=request.conversation_history,
            response_mode=request.response_mode,
            financial_summary=financial_summary,
            page_context=request.page_context
        )

        return ChatbotQueryResponse(**result)

    except Exception as e:
        logger.error(f"Chatbot query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "chatbot_initialized": _chatbot_instance is not None,
        "dependencies_configured": all([
            _get_db_session is not None,
            _get_current_user is not None
        ])
    }


# HTMX-specific endpoints for partial updates
@router.post("/query/htmx", response_class=HTMLResponse)
async def chatbot_query_htmx(
    request: Request,
    db: Session = Depends(get_db_session),
    user_context: dict = Depends(get_current_user)
):
    """
    HTMX-friendly endpoint that returns HTML fragments

    Form data:
    - question: User's question
    - model: LLM provider
    """
    if templates is None:
        raise HTTPException(500, "Templates not configured")

    try:
        form = await request.form()
        question = form.get("question", "")
        model = form.get("model", "ollama:llama3:latest")
        response_mode = form.get("response_mode")  # Get user's choice
        page_context = form.get("page_context")

        if not question:
            return templates.TemplateResponse(
                "ai_chatbot/error_message.html",
                {"request": request, "error": "Please enter a question"}
            )

        # If no response mode selected, show selector
        if not response_mode:
            from datetime import datetime
            return templates.TemplateResponse(
                "ai_chatbot/response_selector.html",
                {
                    "request": request,
                    "question": question,
                    "model": model,
                    "timestamp": datetime.now().strftime("%I:%M:%S %p")
                }
            )

        chatbot = get_chatbot()

        # Get financial summary for RAG context
        try:
            financial_summary = chatbot.get_financial_summary(user_context, db)
        except Exception as e:
            logger.error(f"Failed to generate financial summary: {str(e)}")
            financial_summary = None

        # Get conversation history from session/form if available
        # This is a simplified version - you may want to implement session management
        conversation_history = None

        # Process query with user's chosen response mode
        result = await chatbot.process_query(
            question=question,
            provider_name=model,
            user_context=user_context,
            db_session=db,
            conversation_history=conversation_history,
            response_mode=response_mode,
            financial_summary=financial_summary,
            page_context=page_context
        )

        # Format chart config for frontend
        chart_config_str = None
        if result["chart_config"]:
            chart_gen = ChartGenerator()
            chart_config_str = chart_gen.format_for_frontend(result["chart_config"])

        visualizations_payload = []
        if result.get("visualizations"):
            visualizations_payload = [
                {
                    "chart_config": viz.get("chart_config"),
                    "chart_title": viz.get("chart_title"),
                    "table_data": viz.get("table_data"),
                    "sql_query": viz.get("sql_query"),
                }
                for viz in result["visualizations"]
            ]

        # Return HTML fragment with response
        return templates.TemplateResponse(
            "ai_chatbot/response.html",
            {
                "request": request,
                "response": result["response"],
                "chart_config": chart_config_str,
                "chart_title": result["chart_title"],
                "table_data": result["table_data"],
                "mode": result["mode"],
                "visualizations": visualizations_payload,
            }
        )

    except Exception as e:
        logger.error(f"HTMX chatbot query failed: {str(e)}")
        return templates.TemplateResponse(
            "ai_chatbot/error_message.html",
            {"request": request, "error": str(e)}
        )
