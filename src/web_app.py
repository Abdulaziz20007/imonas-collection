"""
Web application for order management using FastAPI with server-side rendering.
Provides a web interface to view collections and orders.
"""
import logging
import asyncio, datetime
import os
import datetime
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Request, HTTPException, Form, Depends, status
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, Response, JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.datastructures import URL
from fastapi import APIRouter
from pydantic import BaseModel, Field
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
import uvicorn
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature
from src.database.db_service import db_service
from src.processor import message_processor

# Global variable to hold the bot application instance
bot_app = None

def set_bot_app(app_instance):
    """Set the global bot application instance."""
    global bot_app
    bot_app = app_instance

# Import config and config_db_service
from src.config import config
from src.database.config_db_service import config_db_service
import hmac
import hashlib
import time

logger = logging.getLogger(__name__)

# Flash messaging system
def flash(request: Request, message: str, category: str = "primary") -> None:
    if "_messages" not in request.session:
        request.session["_messages"] = []
    request.session["_messages"].append({"message": message, "category": category})

def get_flashed_messages(request: Request):
    return request.session.pop("_messages") if "_messages" in request.session else []

from starlette.middleware.sessions import SessionMiddleware

admin_app = FastAPI(
    title="Buyurtmalar Boshqaruv Tizimi",
    description="Buyurtmalarni kuzatish uchun veb interfeysi",
)
user_app = FastAPI(
    title="Foydalanuvchi Kabineti",
    description="Foydalanuvchilar uchun buyurtmalarni kuzatish interfeysi",
)

admin_app.add_middleware(SessionMiddleware, secret_key=config.SECRET_KEY)
user_app.add_middleware(SessionMiddleware, secret_key=config.SECRET_KEY)

admin_router = APIRouter()
api_router = APIRouter(prefix="/api")
auth_router = APIRouter()
admin_api_router = APIRouter(prefix="/api/admin", tags=["Admin API"])
user_router = APIRouter()
user_api_router = APIRouter(prefix="/api")

class UserLoginRequest(BaseModel):
    code: str
    phone: str

class OrderUpdateRequest(BaseModel):
    code: str
    phone: str
    amount: int

class UserAuthRequest(BaseModel):
    code: str
    phone: str

# --- Pydantic Models for Admin API ---
class User(BaseModel):
    id: int
    telegram_id: int
    username: Optional[str] = None
    name: Optional[str] = None
    surname: Optional[str] = None
    phone: Optional[str] = None
    is_active: bool
    reg_step: str
    code: Optional[str] = None
    subscription_status: str
    target_amount: Optional[float] = None
    paid_amount: float
    created_at: datetime.datetime
    order_count: int
    collections_count: int

class Collection(BaseModel):
    id: int
    status: str
    finish_at: Optional[datetime.datetime] = None
    close_at: Optional[datetime.datetime] = None
    created_at: datetime.datetime
    user_count: int
    order_count: int

class Order(BaseModel):
    id: int
    user_id: int
    collection_id: int
    amount: Optional[int] = None
    status: int
    created_at: datetime.datetime
    user_name: Optional[str] = None
    user_surname: Optional[str] = None
    user_phone: Optional[str] = None
    user_code: Optional[str] = None
    collection_status: str
    file_count: int

class Card(BaseModel):
    id: int
    name: str
    number: str
    is_active: bool
    created_at: datetime.datetime

class DashboardStats(BaseModel):
    total_users: int
    total_orders: int
    total_collections: int
    open_collections: int
    new_users_today: int
    new_orders_today: int

class NewCardRequest(BaseModel):
    name: str
    number: str = Field(..., min_length=16, max_length=16, pattern=r'^\d{16}$')

class ToggleUserActiveResponse(BaseModel):
    id: int
    telegram_id: int
    is_active: bool

# --- Additional Schemas for Admin API ---
class Admin(BaseModel):
    id: int
    username: str
    role: str

class AdminCreate(BaseModel):
    username: str
    password: str
    role: str = Field(..., pattern=r'^(admin|superadmin)$')

class AdminUpdate(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = Field(None, pattern=r'^(admin|superadmin)$')

class CardUpdate(BaseModel):
    name: Optional[str] = None
    number: Optional[str] = Field(None, min_length=16, max_length=16, pattern=r'^\d{16}$')

class AIModel(BaseModel):
    id: str
    name: str

class DefaultAIModel(BaseModel):
    model_id: str

class UserbotStatus(BaseModel):
    state: str
    me: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class UserbotCode(BaseModel):
    code: str

# --- Userbot (Telethon) management state ---
userbot_client: Optional[TelegramClient] = None
userbot_run_task: Optional[asyncio.Task] = None
userbot_login_state: Dict[str, Any] = {}

def _get_userbot_session_path() -> str:
    session_path = "sessions/userbot/userbot"
    os.makedirs(os.path.dirname(session_path), exist_ok=True)
    return session_path

async def validate_product_exists_async(channel_id: int, message_id: int) -> bool:
    """
    Async function to validate if a product exists in the channel.
    Used by concurrent handlers for instant product validation.

    Args:
        channel_id: Channel ID where the product should exist
        message_id: Message ID of the product

    Returns:
        True if product exists, False otherwise
    """
    global userbot_client

    is_connected = userbot_client.is_connected() if userbot_client else False
    logger.debug(f"Userbot connection status for product validation: {is_connected}")
    if not is_connected:
        logger.error("Userbot is not connected. Cannot validate product.")
        return False

    try:
        # Use Telethon's get_messages to check if message exists
        message_check = await userbot_client.get_messages(entity=channel_id, ids=message_id)

        # If message doesn't exist or is deleted, return False
        if not message_check or getattr(message_check, 'deleted', False):
            return False

        return True

    except Exception as e:
        logger.error(f"Userbot validation failed for msg {message_id} in channel {channel_id}: {e}")
        return False

async def get_userbot_status() -> Dict[str, Any]:
    """Get comprehensive userbot status with detailed state information and debugging data."""
    global userbot_client, userbot_run_task

    status: Dict[str, Any] = {
        "connected": False,
        "authorized": False,
        "me": None,
        "state": "DISCONNECTED",  # DISCONNECTED, CONNECTED, AUTHORIZED, ERROR
        "error": None,
        "last_updated": None,
        "debug": {
            "client_exists": userbot_client is not None,
            "task_exists": userbot_run_task is not None,
            "task_done": userbot_run_task.done() if userbot_run_task else None,
            "task_cancelled": userbot_run_task.cancelled() if userbot_run_task else None,
            "task_exception": None
        }
    }

    # Debug: Check task status
    if userbot_run_task:
        try:
            if userbot_run_task.done():
                if userbot_run_task.cancelled():
                    status["debug"]["task_status"] = "CANCELLED"
                elif userbot_run_task.exception():
                    exception = userbot_run_task.exception()
                    status["debug"]["task_status"] = "FAILED"
                    # Ensure exception is properly converted to string
                    status["debug"]["task_exception"] = str(exception) if exception else "Unknown exception"
                    logger.error(f"🐛 DEBUG: Background task failed with exception: {exception}")
                else:
                    status["debug"]["task_status"] = "COMPLETED"
            else:
                status["debug"]["task_status"] = "RUNNING"
        except Exception as e:
            status["debug"]["task_status"] = f"ERROR_CHECKING: {e}"

    # If no client exists, return disconnected status
    if userbot_client is None:
        status["state"] = "DISCONNECTED"
        status["debug"]["reason"] = "No client instance"
        logger.debug("🐛 DEBUG: get_userbot_status - No client instance")
        return status

    try:
        # Check connection status
        # Telethon's is_connected() returns a boolean and should not be awaited
        status["connected"] = userbot_client.is_connected()
        status["debug"]["connection_check"] = "SUCCESS"

        if not status["connected"]:
            status["state"] = "DISCONNECTED"
            status["debug"]["reason"] = "Client exists but not connected"
            logger.debug("🐛 DEBUG: get_userbot_status - Client not connected")
            return status

        status["state"] = "CONNECTED"
        logger.debug("🐛 DEBUG: get_userbot_status - Client connected")

    except Exception as e:
        logger.warning(f"🐛 DEBUG: Failed to check connection status: {e}")
        status["connected"] = False
        status["state"] = "ERROR"
        status["error"] = f"Connection check failed: {str(e)}"
        status["debug"]["connection_check"] = f"FAILED: {e}"
        return status

    try:
        # Check authorization status
        # Telethon's is_user_authorized() is an async method and must be awaited
        status["authorized"] = await userbot_client.is_user_authorized()
        status["debug"]["auth_check"] = "SUCCESS"

        if status["authorized"]:
            status["state"] = "AUTHORIZED"
            logger.debug("🐛 DEBUG: get_userbot_status - Client authorized")
        else:
            logger.debug("🐛 DEBUG: get_userbot_status - Client connected but not authorized")

    except Exception as e:
        logger.warning(f"🐛 DEBUG: Failed to check authorization status: {e}")
        status["authorized"] = False
        status["error"] = f"Authorization check failed: {str(e)}"
        status["debug"]["auth_check"] = f"FAILED: {e}"

        # If we get session conflicts when checking status, reset the client
        if "authorization key" in str(e).lower() and "ip addresses" in str(e).lower():
            logger.error("🔥 Session conflict detected in status check, resetting userbot client")
            status["state"] = "ERROR"
            status["error"] = "Session conflict detected. Please clean session and reconnect."
            status["debug"]["reason"] = "Session conflict detected"
            try:
                await userbot_client.disconnect()
            except Exception:
                pass
            userbot_client = None
            return status

    try:
        # Get user information if authorized
        if status["authorized"]:
            me = await userbot_client.get_me()
            status["me"] = {
                "id": getattr(me, 'id', None),
                "first_name": getattr(me, 'first_name', None),
                "last_name": getattr(me, 'last_name', None),
                "username": getattr(me, 'username', None),
                "phone": getattr(me, 'phone', None)
            }
            status["debug"]["user_info_check"] = "SUCCESS"

            # If phone not available from user object, try to get from settings
            if not status["me"]["phone"]:
                settings = config_db_service.get_all_settings()
                saved_phone = settings.get('userbot_phone_number')
                if saved_phone:
                    status["me"]["phone"] = saved_phone

            # Set display name
            name_parts = []
            if status["me"]["first_name"]:
                name_parts.append(status["me"]["first_name"])
            if status["me"]["last_name"]:
                name_parts.append(status["me"]["last_name"])
            status["me"]["display_name"] = " ".join(name_parts) if name_parts else status["me"]["username"] or "Unknown"

    except Exception as e:
        logger.warning(f"🐛 DEBUG: Failed to get user info: {e}")
        status["debug"]["user_info_check"] = f"FAILED: {e}"

        # If getting user info fails due to session conflict, mark as not authorized
        if "authorization key" in str(e).lower() and "ip addresses" in str(e).lower():
            status["authorized"] = False
            status["state"] = "ERROR"
            status["error"] = "Session conflict when getting user info"
            status["me"] = None
            status["debug"]["reason"] = "Session conflict during user info fetch"
        else:
            # Other errors shouldn't affect authorization status
            status["error"] = f"Failed to get user info: {str(e)}"

    # Set timestamp
    status["last_updated"] = datetime.datetime.now().isoformat()

    # Debug: Check for coroutine objects before returning and clean them up
    def clean_coroutines(obj, path=""):
        """Recursively check for and clean coroutine objects in the status dictionary."""
        import inspect
        if inspect.iscoroutine(obj):
            logger.error(f"🚨 COROUTINE FOUND at {path}: {obj}")
            # Replace coroutine with a safe representation
            return f"<COROUTINE_ERROR: {type(obj).__name__}>"
        elif isinstance(obj, dict):
            cleaned = {}
            for key, value in obj.items():
                cleaned[key] = clean_coroutines(value, f"{path}.{key}" if path else key)
            return cleaned
        elif isinstance(obj, (list, tuple)):
            return [clean_coroutines(value, f"{path}[{i}]" if path else f"[{i}]") for i, value in enumerate(obj)]
        else:
            return obj

    # Clean any coroutines from the status object
    original_status = status.copy() if isinstance(status, dict) else status
    status = clean_coroutines(status)

    # Check if any coroutines were found
    if status != original_status:
        logger.error(f"🚨 CRITICAL: Coroutines were found and cleaned from status object!")
        logger.error(f"🚨 Original problematic object: {original_status}")
    else:
        logger.debug(f"✅ Status object is clean of coroutines")

    logger.debug(f"🐛 DEBUG: Final status - state: {status['state']}, connected: {status['connected']}, authorized: {status['authorized']}")

    return status

def _attach_userbot_handlers(client: TelegramClient) -> None:
    @client.on(events.NewMessage(incoming=True))
    async def bank_notification_handler(event):
        if event.is_private and not event.out:
            try:
                sender_id = None
                try:
                    # Telethon can expose sender on event.message or event directly
                    sender_id = getattr(getattr(event.message, 'sender', None), 'id', None) or \
                                getattr(event.message, 'sender_id', None) or \
                                getattr(event, 'sender_id', None)
                except Exception:
                    pass

                if sender_id and config.ALLOWED_BANK_BOT_IDS and sender_id not in config.ALLOWED_BANK_BOT_IDS:
                    return

                await message_processor.handle_bank_notification(event.message)
            except Exception as e:
                print(f"Error in bank_notification_handler: {e}")

async def startup_userbot():
    """
    Initializes and starts the userbot on application startup if a session file exists.
    """
    global userbot_client, userbot_run_task
    session_path = _get_userbot_session_path()
    session_file = session_path + ".session"

    if not os.path.exists(session_file):
        logger.info("No userbot session file found. Userbot will not start automatically.")
        return

    logger.info("Userbot session file found. Attempting to connect...")
    
    app_id = config.API_ID
    api_hash = config.API_HASH

    if not app_id or not api_hash:
        logger.error("API_ID or API_HASH not configured. Cannot start userbot.")
        return

    try:
        client = TelegramClient(
            session_path,
            app_id,
            api_hash,
            system_version="4.16.30-vxCUSTOM",
            use_ipv6=False
        )
        await client.connect()

        if await client.is_user_authorized():
            userbot_client = client
            _attach_userbot_handlers(userbot_client)
            message_processor.set_userbot(userbot_client)
            
            if userbot_run_task is None or userbot_run_task.done():
                userbot_run_task = asyncio.create_task(userbot_client.run_until_disconnected())
            
            me = await userbot_client.get_me()
            logger.info(f"✅ Userbot automatically connected and started as {me.first_name}")
        else:
            logger.warning("Userbot session is invalid or expired. Please log in again via the admin panel.")
            await client.disconnect()
            _clean_userbot_session_files()
    except Exception as e:
        logger.error(f"Failed to start userbot from session: {e}")
        _clean_userbot_session_files()

async def apply_price_change_to_everyone(new_price: float):
    """
    Apply a new subscription price to all users who are in 'pending_payment' status.
    This is a background task triggered from the admin panel.
    """
    logger.info(f"Applying new price {new_price} to all pending users.")
    pending_users = db_service.get_users_by_subscription_status('pending_payment')
    
    for user_dict in pending_users:
        user = dict(user_dict)
        telegram_id = user['telegram_id']
        
        # Update user's target amount
        db_service.update_user_subscription_amounts(telegram_id, target_amount=new_price)
        
        # Check if user has now paid enough
        if user['paid_amount'] >= new_price:
            db_service.update_user_subscription_status(telegram_id, 'active')
            
            if not bot_app:
                logger.warning(f"bot_app not available. Cannot send message to user {telegram_id}.")
                continue

            try:
                link = await bot_app.bot.create_chat_invite_link(
                    chat_id=config.PRIVATE_CHANNEL_ID,
                    member_limit=1,
                    name=user.get('name', f'User {telegram_id}')
                )
                
                message = f"✅ Admin obuna narxini o'zgartirdi. Sizning to'lovingiz endi yetarli!\n\nKanalga qo'shilish uchun havola:\n{link.invite_link}"
                
                # Handle overpayment
                overpaid_amount = user['paid_amount'] - new_price
                if overpaid_amount > 0.01: # Use a small epsilon for float comparison
                    overpaid_formatted = f"{overpaid_amount:,.0f}".replace(',', ' ')
                    message += f"\n\nSiz {overpaid_formatted} UZS ortiqcha to'lov qildingiz. Pulni qaytarish uchun admin bilan bog'laning."

                await bot_app.bot.send_message(chat_id=telegram_id, text=message)
                logger.info(f"User {telegram_id} automatically subscribed due to price change.")
            except Exception as e:
                logger.error(f"Failed to send message to user {telegram_id} after price change: {e}")


# Setup templates
templates = Jinja2Templates(directory="templates")

# Add flash messages to template context
templates.env.globals['get_flashed_messages'] = get_flashed_messages

# Add custom filters
def format_currency(value):
    """Format currency for display."""
    try:
        return f"{int(value):,}".replace(',', ' ')
    except (ValueError, TypeError):
        return "0"

templates.env.filters['format_currency'] = format_currency

# --- Authentication ---
# Use a distinct cookie name here to avoid clashing with legacy admin session cookie below
ADMIN_JWT_COOKIE_NAME = "admin_jwt"
SESSION_DURATION = 3600 * 8  # 8 hours

s = URLSafeTimedSerializer(config.SECRET_KEY, salt='auth-jwt')

def create_jwt_cookie(response: Response, username: str):
    token = s.dumps(username)
    response.set_cookie(key=ADMIN_JWT_COOKIE_NAME, value=token, max_age=SESSION_DURATION, httponly=True, samesite='lax')

async def get_current_user_from_jwt(request: Request) -> Optional[str]:
    token = request.cookies.get(ADMIN_JWT_COOKIE_NAME)
    if not token:
        return None
    try:
        username = s.loads(token, max_age=SESSION_DURATION)
        return username
    except (SignatureExpired, BadTimeSignature):
        return None

async def verify_jwt(request: Request) -> str:
    user = await get_current_user_from_jwt(request)
    if not user:
        # Build redirect URL with a 'next' parameter
        try:
            next_url = str(request.url)
            redirect_url = f"/auth?next={URL(next_url).path}"
        except Exception:
            redirect_url = "/auth"
        
        raise HTTPException(
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
            headers={"Location": redirect_url},
        )
    return user

@admin_app.middleware("http")
async def add_user_to_request(request: Request, call_next):
    request.state.current_user = await get_current_user_from_jwt(request)
    response = await call_next(request)
    return response

# Exception handlers
@admin_app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions with custom error pages."""
    # Let redirects pass through
    if exc.status_code in (301, 302, 303, 307, 308):
        location = exc.headers.get("Location") if exc.headers else None
        if location:
            return RedirectResponse(url=location, status_code=exc.status_code)
        return Response(status_code=exc.status_code)
    if exc.status_code == 404:
        return templates.TemplateResponse(
            "404.html", 
            {"request": request, "current_user": request.state.current_user}, 
            status_code=404
        )
    # For other HTTP errors, you could create additional error pages
    return templates.TemplateResponse(
        "404.html", 
        {"request": request, "current_user": request.state.current_user, "error_code": exc.status_code, "error_detail": exc.detail}, 
        status_code=exc.status_code
    )

@admin_app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors."""
    return templates.TemplateResponse(
        "404.html", 
        {"request": request, "current_user": request.state.current_user, "error_detail": "Noto'g'ri so'rov formati"}, 
        status_code=400
    )

@user_app.exception_handler(StarletteHTTPException)
async def user_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions for the user app."""
    if exc.status_code == 404:
        return templates.TemplateResponse(
            "user_not_found.html", 
            {"request": request, "current_user": getattr(request.state, 'current_user', None)}, 
            status_code=404
        )
    return HTMLResponse(f"<h1>Xatolik: {exc.status_code}</h1><p>{exc.detail}</p>", status_code=exc.status_code)

@user_app.exception_handler(RequestValidationError)
async def user_validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors for the user app."""
    return JSONResponse(
        status_code=400,
        content={"success": False, "message": "Noto'g'ri so'rov formati"}
    )

# Mount static files and uploads for both apps
if not os.path.exists("static"):
    os.makedirs("static")
if not os.path.exists("uploads"):
    os.makedirs("uploads")

admin_app.mount("/static", StaticFiles(directory="static"), name="static")
user_app.mount("/static", StaticFiles(directory="static"), name="static")

def get_thumbnail_url(file_path: str) -> str:
    """
    Get the thumbnail URL for a file, prioritizing thumbnails for performance.
    Args:
        file_path: Original file path (e.g., 'uploads/order_....mp4')
    Returns:
        Thumbnail path if it exists, otherwise original file path
    """
    if not file_path:
        return ""

    # Extract just the filename from the path
    filename = os.path.basename(file_path)
    name_without_ext = os.path.splitext(filename)[0]

    # Check product thumbnail first (canonical location)
    product_thumb_path = os.path.join("uploads", "products", "thumbnails", f"{name_without_ext}.webp")
    if os.path.exists(product_thumb_path):
        return product_thumb_path

    # Check legacy/global thumbnail
    thumbnail_path = f"uploads/thumbnail/{name_without_ext}.webp"
    if os.path.exists(thumbnail_path):
        return thumbnail_path

    # For images, return original path
    if filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
        return file_path

    # For videos without thumbnails, return empty (will show placeholder)
    return ""

def get_collections_with_user_counts():
    """Get all collections with user counts who have orders."""
    try:
        conn = db_service._get_connection()
        cursor = conn.cursor()

        # Get collections with user counts and order counts
        sql = """
            SELECT c.*,
                   COUNT(DISTINCT o.user_id) as user_count,
                   COUNT(o.id) as order_count
            FROM collections c
            LEFT JOIN orders o ON c.id = o.collection_id
            GROUP BY c.id, c.status, c.created_at, c.close_at, c.finish_at
            ORDER BY c.created_at DESC
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Error getting collections: {e}")
        return []

def get_collection_users_with_orders(collection_id: int):
    """Get all users who have orders in a specific collection."""
    try:
        conn = db_service._get_connection()
        cursor = conn.cursor()
        
        sql = """
            SELECT u.id, u.name, u.surname, u.phone, 
                   u.code, COUNT(o.id) as order_count,
                   MIN(o.id) as first_order_id
            FROM users u
            JOIN orders o ON u.id = o.user_id
            WHERE o.collection_id = ?
            GROUP BY u.id, u.name, u.surname, u.phone, u.code
            ORDER BY u.name, u.surname
        """
        cursor.execute(sql, (collection_id,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Error getting collection users: {e}")
        return []


def get_user_orders_for_collection(user_id: int, collection_id: int):
    """Deprecated: Use db_service.get_user_orders_by_collection instead."""
    return db_service.get_user_orders_by_collection(user_id, collection_id, limit=100)

def get_authenticated_user(code: str, phone: str) -> Optional[Dict[str, Any]]:
    user = db_service.get_user_by_code(code)
    if not user:
        return None
    
    db_phone = user.get('phone', '')
    
    # Normalize both phone numbers to be digits-only for comparison
    db_phone_digits = ''.join(filter(str.isdigit, db_phone))
    input_phone_digits = ''.join(filter(str.isdigit, phone))
    
    if db_phone_digits and input_phone_digits and db_phone_digits == input_phone_digits:
        return user
    return None

# --- Admin-only OpenAPI and Swagger UI ---
@admin_app.get("/openapi-admin.json", include_in_schema=False)
async def openapi_admin_schema(current_user: str = Depends(verify_jwt)):
    """Return OpenAPI schema filtered to only Admin API endpoints (tagged 'Admin API')."""
    openapi_schema = admin_app.openapi()
    # Filter paths to only those that have at least one operation with tag 'Admin API'
    filtered_paths = {}
    for path, methods in openapi_schema.get("paths", {}).items():
        include_path = False
        new_methods = {}
        for method, operation in methods.items():
            if isinstance(operation, dict) and 'tags' in operation and 'Admin API' in operation['tags']:
                include_path = True
                new_methods[method] = operation
        if include_path and new_methods:
            filtered_paths[path] = new_methods
    openapi_schema['paths'] = filtered_paths
    return JSONResponse(openapi_schema)

@admin_app.get("/swagger", include_in_schema=False)
async def swagger_ui(current_user: str = Depends(verify_jwt)):
    """Serve Swagger UI that points to the admin-only OpenAPI schema."""
    return get_swagger_ui_html(
        openapi_url="/openapi-admin.json",
        title="Admin API - Swagger UI"
    )

# --- New Admin API Endpoints ---

@admin_api_router.get("/stats", response_model=DashboardStats)
async def get_dashboard_stats_api():
    """Get main dashboard statistics."""
    stats = db_service.get_dashboard_stats()
    return stats

@admin_api_router.get("/users", response_model=List[User])
async def get_users_api_list():
    """Get a list of all users with their statistics."""
    users_with_stats = db_service.get_all_users_with_stats()
    return users_with_stats

@admin_api_router.post("/users/{user_id}/toggle-active", response_model=ToggleUserActiveResponse)
async def toggle_user_active_api(user_id: int):
    """Toggle the active status of a user (ban/unban)."""
    user = db_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    new_status = not user['is_active']
    success = db_service.update_user_active_status(user['telegram_id'], new_status)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update user status")
    
    updated_user = db_service.get_user_by_id(user_id)
    if not updated_user:
        raise HTTPException(status_code=404, detail="User not found after update")
        
    return ToggleUserActiveResponse(id=updated_user['id'], telegram_id=updated_user['telegram_id'], is_active=updated_user['is_active'])

@admin_api_router.get("/collections", response_model=List[Collection])
async def get_collections_api():
    """Get a list of all collections with their statistics."""
    collections = get_collections_with_user_counts()
    return collections

@admin_api_router.post("/collections", response_model=Collection)
async def create_collection_api():
    """Create a new collection, which also closes the currently active one."""
    if db_service.has_close_collections():
        raise HTTPException(status_code=400, detail="There are closed collections that must be finished first.")
    
    new_collection_id = db_service.atomically_open_new_collection()
    if not new_collection_id:
        raise HTTPException(status_code=500, detail="Failed to create new collection")
    
    async def notify_users_task(collection_id):
        if not bot_app:
            logger.warning("bot_app not available. Skipping user notifications for new collection.")
            return
        
        registered_users = db_service.get_users_by_registration_step('done')
        for user in registered_users:
            if not user.get('code'):
                continue
            try:
                unique_code = f"{collection_id}-{user['code']}"
                message_text = f"🎉 Yangi kolleksiya ochildi!\n\n🌟 Sizning yangi unikal kodingiz: `{unique_code}`"
                sent_message = await bot_app.bot.send_message(chat_id=user['telegram_id'], text=message_text, parse_mode="Markdown")
                await bot_app.bot.pin_chat_message(chat_id=user['telegram_id'], message_id=sent_message.message_id, disable_notification=True)
            except Exception as e:
                logger.error(f"Failed to send/pin code for user {user['telegram_id']}: {e}")

    asyncio.create_task(notify_users_task(new_collection_id))
    
    new_collection_data = get_collections_with_user_counts()
    new_collection = next((c for c in new_collection_data if c['id'] == new_collection_id), None)
    if not new_collection:
        raise HTTPException(status_code=404, detail="Newly created collection not found")
        
    return new_collection

@admin_api_router.post("/collections/{collection_id}/reopen", response_model=Collection)
async def reopen_collection_api(collection_id: int):
    """Reopen a 'closed' collection; merges current 'open' into it and sets it to 'open'."""
    target = db_service.get_collection_by_id(collection_id)
    if not target:
        raise HTTPException(status_code=404, detail="Collection not found")
    if target.get('status') != 'close':
        raise HTTPException(status_code=400, detail="Collection is not in 'close' status")

    # Merge current open collection into the target (if exists)
    current_open = db_service.get_active_collection()
    if current_open and current_open['id'] != collection_id:
        db_service.merge_collections(from_collection_id=current_open['id'], to_collection_id=collection_id)
        db_service.delete_collection(current_open['id'])

    # Reopen the target collection
    ok = db_service.update_collection_status(collection_id, 'open')
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to reopen collection")
    # Return with stats
    updated = db_service.get_collection_by_id(collection_id)
    # Attach counts similarly to get_collections_with_user_counts
    collections = get_collections_with_user_counts()
    with_counts = next((c for c in collections if c['id'] == collection_id), None)
    return with_counts or updated

@admin_api_router.post("/collections/{collection_id}/finish", response_model=Collection)
async def finish_collection_api(collection_id: int):
    """Mark a 'closed' collection as 'finish'."""
    target = db_service.get_collection_by_id(collection_id)
    if not target:
        raise HTTPException(status_code=404, detail="Collection not found")
    if target.get('status') != 'close':
        raise HTTPException(status_code=400, detail="Collection must be 'close' to finish")
    ok = db_service.update_collection_status(collection_id, 'finish')
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to finish collection")
    collections = get_collections_with_user_counts()
    with_counts = next((c for c in collections if c['id'] == collection_id), None)
    return with_counts or db_service.get_collection_by_id(collection_id)

@admin_api_router.get("/orders", response_model=List[Order])
async def get_orders_api_list():
    """Get a list of all orders with user and collection details."""
    orders_raw = db_service.get_all_orders_with_details(limit=200)
    orders_transformed = []
    for o in orders_raw:
        order_dict = dict(o)
        order_dict['user_name'] = order_dict.pop('name', None)
        order_dict['user_surname'] = order_dict.pop('surname', None)
        order_dict['user_phone'] = order_dict.pop('phone', None)
        order_dict['user_code'] = order_dict.pop('code', None)
        orders_transformed.append(order_dict)
    return orders_transformed

@admin_api_router.get("/cards", response_model=List[Card])
async def get_cards_api():
    """Get a list of all payment cards."""
    return db_service.get_all_cards()

@admin_api_router.post("/cards", response_model=Card)
async def add_card_api(card_data: NewCardRequest):
    """Add a new payment card."""
    card_id = db_service.add_card(card_data.name, card_data.number)
    if not card_id:
        raise HTTPException(status_code=400, detail="Card with this number may already exist.")
    
    all_cards = db_service.get_all_cards()
    new_card = next((c for c in all_cards if c['id'] == card_id), None)
    if not new_card:
        raise HTTPException(status_code=404, detail="Newly created card not found")
    return new_card

# --- New endpoints to appear in Swagger (Admin API) ---

@admin_api_router.patch("/cards/{card_id}", response_model=Card)
async def edit_card_api(card_id: int, update: CardUpdate):
    """Edit a payment card's name and/or number."""
    existing = next((c for c in db_service.get_all_cards() if c['id'] == card_id), None)
    if not existing:
        raise HTTPException(status_code=404, detail="Card not found")
    name = update.name if update.name is not None else existing['name']
    number = update.number if update.number is not None else existing['number']
    ok = db_service.update_card(card_id, name, number)
    if not ok:
        raise HTTPException(status_code=400, detail="Failed to update card (possibly duplicate number)")
    updated = next((c for c in db_service.get_all_cards() if c['id'] == card_id), None)
    return updated

@admin_api_router.get("/ai/models", response_model=List[AIModel])
async def get_ai_models_api():
    """Get list of available AI models."""
    # Static list could be loaded from settings/config; keeping simple for now
    models = [
        {"id": "gemini-2.0-flash", "name": "Gemini 2.0 Flash"},
        {"id": "gemini-2.5-flash-lite", "name": "Gemini 2.5 Flash Lite"},
    ]
    return models

@admin_api_router.post("/ai/models/default", response_model=DefaultAIModel)
async def set_default_ai_model_api(payload: DefaultAIModel, request: Request):
    """Set default AI model in configuration settings."""
    ok = config_db_service.update_settings({"default_ai_model": payload.model_id}, "api")
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update default AI model")
    return payload

@admin_api_router.get("/userbot/status", response_model=UserbotStatus)
async def get_userbot_status_api():
    status = await get_userbot_status()
    return {
        "state": status.get("state"),
        "me": status.get("me"),
        "error": status.get("error"),
    }

@admin_api_router.post("/userbot/login/send-code")
async def userbot_send_code_api(request: Request):
    """Initiate userbot login by sending verification code to configured phone."""
    return await api_userbot_send_code(request)

@admin_api_router.post("/userbot/login/verify-code")
async def userbot_verify_code_api(request: Request):
    """Verify the userbot login code and complete login."""
    return await api_userbot_verify_code(request)

@admin_api_router.post("/userbot/logout")
async def userbot_logout_api(request: Request):
    """Logout userbot and clean session."""
    return await api_userbot_logout(request)

@admin_api_router.delete("/cards/{card_id}", status_code=204)
async def delete_card_api(card_id: int):
    """Delete a payment card. Cannot delete last or active card."""
    # Validate constraints inside service: it returns False on violations
    ok = db_service.delete_card(card_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Cannot delete this card. Ensure it's not active and not the last card.")
    return Response(status_code=204)

@admin_api_router.post("/cards/{card_id}/activate", response_model=Card)
async def activate_card_api(card_id: int):
    """Activate a card and deactivate others."""
    # Ensure card exists
    existing = next((c for c in db_service.get_all_cards() if c['id'] == card_id), None)
    if not existing:
        raise HTTPException(status_code=404, detail="Card not found")
    ok = db_service.set_active_card(card_id)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to activate card")
    updated = next((c for c in db_service.get_all_cards() if c['id'] == card_id), None)
    return updated

@admin_api_router.get("/admins", response_model=List[Admin])
async def get_admins_api(page: int = 1, size: int = 20):
    """Retrieve a paginated list of web panel administrators."""
    admins = config_db_service.get_all_admins()
    # Simple pagination
    start = max((page - 1) * size, 0)
    end = start + size
    return admins[start:end]

@admin_api_router.post("/admins", response_model=Admin, status_code=201)
async def add_admin_api(payload: AdminCreate, request: Request):
    """Create a new web panel administrator."""
    admin_id = config_db_service.add_admin(payload.username, payload.password, payload.role, "api")
    if not admin_id:
        raise HTTPException(status_code=400, detail="Username already exists")
    admin = config_db_service.get_admin_by_id(admin_id)
    if not admin:
        raise HTTPException(status_code=404, detail="Admin not found after creation")
    return admin

@admin_api_router.patch("/admins/{admin_id}", response_model=Admin)
async def edit_admin_api(admin_id: int, payload: AdminUpdate, request: Request):
    """Update an existing administrator's fields."""
    ok = config_db_service.update_admin(
        admin_id,
        username=payload.username,
        password=payload.password,
        role=payload.role,
        admin_user="api",
    )
    if not ok:
        raise HTTPException(status_code=400, detail="Failed to update admin (possibly duplicate username or no changes)")
    admin = config_db_service.get_admin_by_id(admin_id)
    if not admin:
        raise HTTPException(status_code=404, detail="Admin not found")
    return admin

@admin_api_router.delete("/admins/{admin_id}", status_code=204)
async def delete_admin_api(admin_id: int, request: Request):
    """Delete a web panel administrator."""
    ok = config_db_service.delete_admin(admin_id, "api")
    if not ok:
        raise HTTPException(status_code=404, detail="Admin not found")
    return Response(status_code=204)

@user_router.get("/", response_class=HTMLResponse)
async def landing_page(request: Request):
    """Landing page - Professional order management system introduction"""
    return templates.TemplateResponse("landing.html", {"request": request})

@user_router.get("/{code}/")
async def user_page_redirect(code: str):
    """Redirect from /user/code/ to /user/code for consistency"""
    return RedirectResponse(url=f"/{code}", status_code=301)

@user_router.get("/{code}", response_class=HTMLResponse)
async def user_page(request: Request, code: str):
    user = db_service.get_user_by_code(code)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return templates.TemplateResponse("user_page.html", {"request": request, "user": user})

@user_api_router.post("/login")
async def user_login(request: UserLoginRequest):
    user = get_authenticated_user(request.code, request.phone)
    if not user:
        raise HTTPException(status_code=403, detail="Invalid credentials")
    
    orders = db_service.get_user_orders_with_files(user['id'], limit=100)
    
    # Add collection status to orders and filter out cancelled orders
    active_orders = []
    for order in orders:
        # Only include active orders (status = 1)
        if order.get('status', 1) == 1:
            collection = db_service.get_collection_by_id(order['collection_id'])
            order['collection_status'] = collection['status'] if collection else 'unknown'
            active_orders.append(order)

    # Get collection summaries for this user
    collections = db_service.get_user_collections_summary(user['id'])

    return {
        "success": True,
        "user": {
            "name": user.get('name'),
            "surname": user.get('surname'),
            "phone": user.get('phone'),
            "code": user.get('code')
        },
        "orders": active_orders,
        "collections": collections
    }

@user_api_router.post("/orders/{order_id}/update")
async def update_order(order_id: int, request: OrderUpdateRequest):
    user = get_authenticated_user(request.code, request.phone)
    if not user:
        raise HTTPException(status_code=403, detail="Invalid credentials")
    
    order = db_service.get_order_by_id(order_id)
    if not order or order['user_id'] != user['id']:
        raise HTTPException(status_code=404, detail="Order not found or does not belong to user")

    collection = db_service.get_collection_by_id(order['collection_id'])
    if not collection or collection['status'] != 'open':
        raise HTTPException(status_code=400, detail="Cannot edit order in a closed or finished collection")

    success = db_service.update_order_amount(order_id, request.amount)
    if success:
        # Check if all files are downloaded and send notification if ready
        statuses = db_service.get_order_file_statuses(order_id)
        if statuses and all(s == 'downloaded' for s in statuses) and bot_app:
            try:
                # Create a mock context for the notification
                class MockContext:
                    def __init__(self, bot_instance):
                        self.bot = bot_instance
                
                mock_context = MockContext(bot_app)
                
                # Import here to avoid circular imports
                from processor import message_processor
                async def attempt_finalization():
                    finalized = await message_processor.attempt_to_finalize_order(order_id, mock_context)
                    if finalized:
                        logger.info(f"Triggered final notification for order {order_id} updated via web API")
                    else:
                        logger.debug(f"Order {order_id} not ready for final notification via web API")
                asyncio.create_task(attempt_finalization())
            except Exception as notification_error:
                logger.error(f"Failed to send realtime notification for web-updated order {order_id}: {notification_error}")
        
        return {"success": True, "message": "Order updated successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to update order")

@user_api_router.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: int, request: UserAuthRequest):
    user = get_authenticated_user(request.code, request.phone)
    if not user:
        raise HTTPException(status_code=403, detail="Invalid credentials")

    order = db_service.get_order_by_id(order_id)
    if not order or order['user_id'] != user['id']:
        raise HTTPException(status_code=404, detail="Order not found or does not belong to user")

    # Check if order is already cancelled
    if order.get('status', 1) == 0:
        raise HTTPException(status_code=400, detail="Order is already cancelled")

    collection = db_service.get_collection_by_id(order['collection_id'])
    if not collection or collection['status'] != 'open':
        raise HTTPException(status_code=400, detail="Cannot cancel order in a closed or finished collection")

    success = db_service.cancel_order(order_id, user['id'])
    if success:
        return {"success": True, "message": "Order cancelled successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to cancel order")

class UserProfileUpdateRequest(BaseModel):
    code: str
    phone: str
    name: str
    surname: str

@user_api_router.post("/profile/update")
async def update_user_profile(request: UserProfileUpdateRequest):
    user = get_authenticated_user(request.code, request.phone)
    if not user:
        raise HTTPException(status_code=403, detail="Invalid credentials")
    
    # Validate input
    name = request.name.strip()
    surname = request.surname.strip()
    
    if not name or len(name) < 2:
        raise HTTPException(status_code=400, detail="Name must be at least 2 characters long")
    
    if not surname or len(surname) < 2:
        raise HTTPException(status_code=400, detail="Surname must be at least 2 characters long")
    
    # Check for invalid characters (only letters, spaces, and common name characters)
    import re
    if not re.match(r"^[a-zA-ZÀ-ÿ\u00C0-\u024F\u1E00-\u1EFF\s\-'\.]+$", name):
        raise HTTPException(status_code=400, detail="Name contains invalid characters")
    
    if not re.match(r"^[a-zA-ZÀ-ÿ\u00C0-\u024F\u1E00-\u1EFF\s\-'\.]+$", surname):
        raise HTTPException(status_code=400, detail="Surname contains invalid characters")
    
    # Update user profile
    success = db_service.update_user_details(user['id'], name, surname, user.get('phone', ''))
    if success:
        # Also update the individual fields for backward compatibility
        db_service.update_user_info(user['telegram_id'], 'name', name)
        db_service.update_user_info(user['telegram_id'], 'surname', surname)
        
        return {
            "success": True, 
            "message": "Profile updated successfully",
            "user": {
                "name": name,
                "surname": surname,
                "phone": user.get('phone'),
                "code": user.get('code')
            }
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to update profile")

@user_api_router.get("/user/{user_code}/orders")
async def get_user_orders_by_collection(user_code: str, collection_id: int):
    """Get orders for a specific user in a specific collection."""
    try:
        # Get user by code
        user = db_service.get_user_by_code(user_code)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get orders for this user and collection using efficient DB method
        orders = db_service.get_user_orders_by_collection(user['id'], collection_id, limit=100)
        
        # Filter out cancelled orders (status != 1)
        active_orders = []
        for order in orders:
            if order.get('status', 1) == 1:
                active_orders.append(order)
        
        return {
            "success": True,
            "orders": active_orders,
            "count": len(active_orders)
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting user orders by collection: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")



@admin_router.get("/", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def dashboard(request: Request):
    """Main dashboard showing key metrics and recent activity."""
    stats = db_service.get_dashboard_stats()
    recent_orders = db_service.get_recent_orders(limit=5)
    recent_users = db_service.get_recent_users(limit=5)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "stats": stats,
            "recent_orders": recent_orders,
            "recent_users": recent_users,
            "current_user": request.state.current_user,
        },
    )

@admin_router.get("/collections", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def collections_list(request: Request):
    """Main page showing all collections."""
    collections = get_collections_with_user_counts()
    
    # Add status styling info
    for collection in collections:
        if collection['status'] == 'open':
            collection['status_class'] = 'status-open'
            collection['status_text'] = 'OCHIQ'
            collection['status_icon'] = '🟢'
        elif collection['status'] == 'close':
            collection['status_class'] = 'status-close'
            collection['status_text'] = 'YOPIQ'
            collection['status_icon'] = '🔴'
        elif collection['status'] == 'finish':
            collection['status_class'] = 'status-finish'
            collection['status_text'] = 'YAKUNLANGAN'
            collection['status_icon'] = '✅'
        else:
            collection['status_class'] = 'status-unknown'
            collection['status_text'] = 'NOMA\'LUM'
            collection['status_icon'] = '⚪'
    
    return templates.TemplateResponse(
        "collections.html",
        {"request": request, "collections": collections, "current_user": request.state.current_user}
    )

@admin_router.get("/users", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def users_list(request: Request):
    """Users page showing all users with their statistics."""
    users = db_service.get_all_users_with_stats()
    
    # Add user status info and formatting
    for user in users:
        # Ensure plain user_code is available for UI (and Orders filter prefill)
        # In Users page we don't have collection context, so expose base code
        user['user_code'] = user.get('code') or ''

        # Format registration status
        if user['reg_step'] == 'done':
            user['reg_status'] = 'Completed'
            user['reg_status_class'] = 'success'
            user['reg_status_icon'] = '✅'
        else:
            user['reg_status'] = f'Step: {user["reg_step"].title()}'
            user['reg_status_class'] = 'warning'
            user['reg_status_icon'] = '⏳'
        
        # Format active status
        user['active_status'] = 'Active' if user['is_active'] else 'Inactive'
        user['active_status_class'] = 'success' if user['is_active'] else 'secondary'
        user['active_status_icon'] = '✅' if user['is_active'] else '⚪'
        
        # Format latest order date
        if user['latest_order_date']:
            user['latest_order_formatted'] = user['latest_order_date'][:10]
        else:
            user['latest_order_formatted'] = 'No orders'
    
    return templates.TemplateResponse(
        "users.html", 
        {"request": request, "users": users, "current_user": request.state.current_user}
    )

@admin_router.get("/orders", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def orders_list(request: Request):
    """Orders page showing all orders with real-time updates."""
    # Get query parameters
    search_param = request.query_params.get("search", "")
    collection_param = request.query_params.get("collection", "")
    status_param = request.query_params.get("status", "")

    # Handle special 'current' collection parameter
    if collection_param == "current":
        active_collection = db_service.get_active_collection()
        if active_collection:
            collection_param = str(active_collection['id'])
        else:
            collection_param = ""

    # Get all orders first
    orders = db_service.get_all_orders_with_details(limit=100)

    # Apply server-side filtering if parameters are provided
    filtered_orders = []
    for order in orders:
        # Search filter - check name, phone, username, code
        if search_param:
            search_text = f"{order.get('name', '')} {order.get('surname', '')} {order.get('phone', '')} {order.get('username', '')} {order.get('code', '')}".lower()
            if search_param.lower() not in search_text:
                continue

        # Collection filter
        if collection_param and str(order.get('collection_id', '')) != collection_param:
            continue

        # Status filter - filter by collection_status
        if status_param and order.get('collection_status', '') != status_param:
            continue

        filtered_orders.append(order)

    orders = filtered_orders
    
    # Add status styling and formatting
    for order in orders:
        # Format collection status
        order['file_count'] = order.get('file_count', 0)
        if order['collection_status'] == 'open':
            order['status_class'] = 'success'
            order['status_text'] = 'OCHIQ'
            order['status_icon'] = '🟢'
        elif order['collection_status'] == 'close':
            order['status_class'] = 'warning'
            order['status_text'] = 'YOPIQ'
            order['status_icon'] = '🔴'
        elif order['collection_status'] == 'finish':
            order['status_class'] = 'primary'
            order['status_text'] = 'YAKUNLANGAN'
            order['status_icon'] = '✅'
        else:
            order['status_class'] = 'secondary'
            order['status_text'] = 'NOMA\'LUM'
            order['status_icon'] = '⚪'
        
        # Format user name
        user_name = []
        if order.get('name'):
            user_name.append(order['name'])
        if order.get('surname'):
            user_name.append(order['surname'])
        order['user_full_name'] = ' '.join(user_name) if user_name else 'N/A'

        # Format user code
        if order.get('code') and order.get('collection_id'):
            order['user_code'] = f"{order['collection_id']}-{order['code']}"
        else:
            order['user_code'] = ''

        # Format order date if available
        if order.get('collection_created_at'):
            order['order_date_formatted'] = order['collection_created_at'][:10]
        else:
            order['order_date_formatted'] = 'N/A'

        # Add display image URL for thumbnails
        first_file_url = order.get('first_file_url')
        if first_file_url:
            order['display_image_url'] = get_thumbnail_url(first_file_url)
        else:
            order['display_image_url'] = ''

    collection_ids = sorted(list(set(o['collection_id'] for o in orders if o.get('collection_id'))))
    return templates.TemplateResponse(
        "orders.html",
        {
            "request": request,
            "orders": orders,
            "collection_ids": collection_ids,
            "current_user": request.state.current_user,
            "search_param": search_param,
            "collection_param": collection_param,
            "status_param": status_param
        }
    )

@admin_router.get("/collection/{collection_id}", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def collection_detail(request: Request, collection_id: int):
    """Collection detail page showing users with orders."""
    # Get collection info
    collections = get_collections_with_user_counts()
    collection = next((c for c in collections if c['id'] == collection_id), None)
    
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")
    
    # Add status info
    if collection['status'] == 'open':
        collection['status_class'] = 'status-open'
        collection['status_text'] = 'OCHIQ'
        collection['status_icon'] = '🟢'
    elif collection['status'] == 'close':
        collection['status_class'] = 'status-close'
        collection['status_text'] = 'YOPIQ'
        collection['status_icon'] = '🔴'
    elif collection['status'] == 'finish':
        collection['status_class'] = 'status-finish'
        collection['status_text'] = 'YAKUNLANGAN'
        collection['status_icon'] = '✅'
    else:
        collection['status_class'] = 'status-unknown'
        collection['status_text'] = 'NOMA\'LUM'
        collection['status_icon'] = '⚪'
    
    # Get users with orders
    users = get_collection_users_with_orders(collection_id)
    
    # Get orders for each user using efficient DB method
    for user in users:
        user['orders'] = db_service.get_user_orders_by_collection(user['id'], collection_id, limit=100)
        # Add display image URLs for each order
        for order in user['orders']:
            files = order.get('files', [])
            if files:
                order['display_image_url'] = get_thumbnail_url(files[0])
            else:
                order['display_image_url'] = ''
    
    return templates.TemplateResponse(
        "collection_detail.html",
        {
            "request": request, 
            "collection": collection, 
            "users": users,
            "current_user": request.state.current_user,
        }
    )

@admin_router.post("/users/{user_id}/edit", dependencies=[Depends(verify_jwt)])
async def edit_user(request: Request, user_id: int, name: str = Form(...), surname: str = Form(...), phone: str = Form(...)):
    """Handle user detail updates."""
    success = db_service.update_user_details(user_id, name, surname, phone)
    if success:
        flash(request, "Foydalanuvchi ma'lumotlari muvaffaqiyatli yangilandi.", "success")
    else:
        flash(request, "Foydalanuvchi ma'lumotlarini yangilashda xatolik.", "danger")
    return RedirectResponse(url="/users", status_code=303)

@admin_router.post("/users/{user_id}/toggle_active", dependencies=[Depends(verify_jwt)])
async def toggle_user_active(request: Request, user_id: int):
    """Handle banning/unbanning a user."""
    user = db_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    new_status = not user['is_active']
    success = db_service.update_user_active_status(user['telegram_id'], new_status)
    
    if success:
        action = "blokdan chiqarildi" if new_status else "bloklandi"
        flash(request, f"Foydalanuvchi muvaffaqiyatli {action}.", "success")
    else:
        flash(request, "Foydalanuvchi holatini o'zgartirishda xatolik.", "danger")
    return RedirectResponse(url="/users", status_code=303)

@admin_router.post("/collections/create", dependencies=[Depends(verify_jwt)])
async def create_collection(request: Request):
    """Handle creating a new collection."""
    # Prevent creating a new collection if there are any already closed collections
    try:
        if db_service.has_close_collections():
            close_collections = db_service.get_close_collections() or []
            close_collections_text = ", ".join([f"#{c['id']}" for c in close_collections]) if close_collections else ""
            flash(
                request,
                (
                    "❌ Yangi kolleksiya yarata olmaysiz! Avval yopilgan kolleksiyalarni tugatish kerak."
                    + (f"\n\n🔒 Yopilgan kolleksiyalar: {close_collections_text}" if close_collections_text else "")
                ),
                "danger",
            )
            return RedirectResponse(url="/collections", status_code=303)
    except Exception:
        # If the check fails for any reason, fall back to safe behavior (block creation)
        flash(request, "Kolleksiya holatini tekshirishda xatolik. Iltimos, keyinroq urinib ko'ring.", "danger")
        return RedirectResponse(url="/collections", status_code=303)

    # Atomic rotation: close current open collection and open a new one
    new_collection_id = db_service.atomically_open_new_collection()
    if new_collection_id:
        flash(request, f"Yangi kolleksiya #{new_collection_id} muvaffaqiyatli yaratildi.", "success")
        # User notification logic could be added here if bot_app is available
    else:
        flash(request, "Yangi kolleksiya yaratishda xatolik.", "danger")
    return RedirectResponse(url="/collections", status_code=303)

@admin_router.post("/collections/set_status", dependencies=[Depends(verify_jwt)])
async def set_collection_status(request: Request, collection_id: int = Form(...), status: str = Form(...)):
    """Handle changing a collection's status."""
    if status not in ['open', 'finish']:
        raise HTTPException(status_code=400, detail="Invalid status")
    
    if status == 'open':
        # To check the current status, we need to fetch the collection
        all_collections = get_collections_with_user_counts()
        collection_to_reopen = next((c for c in all_collections if c['id'] == collection_id), None)

        if collection_to_reopen:
            if collection_to_reopen['status'] == 'finish':
                flash(request, f"Yakunlangan kolleksiya #{collection_id} qayta ochilmaydi.", "danger")
                return RedirectResponse(url="/collections", status_code=303)

            if collection_to_reopen['status'] == 'close':
                # This is a reopen action. Find the current active collection to merge from.
                active_collection = db_service.get_active_collection()
                if active_collection and active_collection['id'] != collection_id:
                    # Merge orders from the active collection into the one being reopened
                    merge_success = db_service.merge_collections(
                        from_collection_id=active_collection['id'],
                        to_collection_id=collection_id
                    )
                    if not merge_success:
                        flash(request, "Xatolik: Aktiv kolleksiyani birlashtirib bo'lmadi.", "danger")
                        return RedirectResponse(url="/collections", status_code=303)
                    
                    # If merge is successful, delete the old active collection
                    db_service.delete_collection(active_collection['id'])
                    logger.info(f"Successfully merged and deleted collection #{active_collection['id']} into #{collection_id}")

    success = db_service.update_collection_status(collection_id, status)
    if success:
        flash(request, f"Kolleksiya #{collection_id} holati '{status}' ga o'zgartirildi.", "success")
    else:
        flash(request, "Kolleksiya holatini o'zgartirishda xatolik.", "danger")
    return RedirectResponse(url="/collections", status_code=303)

@admin_router.get("/cards", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def cards_management(request: Request):
    """Card management page."""
    cards = db_service.get_all_cards()
    return templates.TemplateResponse( 
        "cards.html",
        {"request": request, "cards": cards}
    )

@admin_router.post("/cards/add", dependencies=[Depends(verify_jwt)])
async def add_card(request: Request, name: str = Form(...), number: str = Form(...)):
    """Handle adding a new card."""
    # Normalize number: remove spaces
    normalized = ''.join(ch for ch in number if ch.isdigit())
    # Determine if this will be the first card
    existing = db_service.get_all_cards()
    will_be_first = len(existing) == 0
    new_id = db_service.add_card(name, normalized)
    # Safety: if this was the first card, ensure it's active
    if will_be_first and new_id:
        db_service.set_active_card(new_id)
    return RedirectResponse(url="/cards", status_code=303)

@admin_router.post("/cards/edit/{card_id}", dependencies=[Depends(verify_jwt)])
async def edit_card(request: Request, card_id: int, name: str = Form(None), number: str = Form(None)):
    """Edit an existing card (name and/or number)."""
    name = name.strip() if isinstance(name, str) else None
    number = ''.join(ch for ch in number if ch.isdigit()) if isinstance(number, str) else None
    ok = False
    if name and number:
        ok = db_service.update_card(card_id, name, number)
    elif name:
        ok = db_service.update_card_name(card_id, name)
    elif number:
        ok = db_service.update_card_number(card_id, number)
    # flash optional
    if ok:
        flash(request, "Karta yangilandi", "success")
    else:
        flash(request, "Karta yangilashda xatolik", "danger")
    return RedirectResponse(url="/cards", status_code=303)

@admin_router.post("/cards/delete/{card_id}", dependencies=[Depends(verify_jwt)])
async def delete_card(request: Request, card_id: int):
    """Handle deleting a card."""
    db_service.delete_card(card_id)
    return RedirectResponse(url="/cards", status_code=303)

@admin_router.post("/cards/set-active/{card_id}", dependencies=[Depends(verify_jwt)])
async def set_active_card(request: Request, card_id: int):
    """Handle setting a card as active."""
    success = db_service.set_active_card(card_id)
    
    if success and bot_app:
        try:
            # Run notification in the background 
            import asyncio
            asyncio.create_task(message_processor.notify_users_of_card_change(bot_app))
        except Exception as e:
            print(f"Error scheduling notification task from web: {e}")
            
    return RedirectResponse(url="/cards", status_code=303)


# --- Admin Panel ---
@auth_router.get("/auth", response_class=HTMLResponse)
async def auth_page(request: Request, next: Optional[str] = None):
    return templates.TemplateResponse("auth.html", {
        "request": request,
        "next": next or "/admin",
        "current_user": None,
    })


@admin_router.get("/settings", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def settings_telegram(request: Request):
    """Telegram settings page."""
    current_user = request.state.current_user
    if not current_user:
        return RedirectResponse(url="/auth")

    context = {
        "request": request,
        "current_user": current_user,
        "current_tab": "telegram",
        "settings": config_db_service.get_all_settings(),
    }
    return templates.TemplateResponse("settings_telegram.html", context)

@admin_router.get("/settings/userbot", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def settings_userbot(request: Request):
    """Userbot settings page."""
    current_user = request.state.current_user
    if not current_user:
        return RedirectResponse(url="/auth")

    context = {
        "request": request,
        "current_user": current_user,
        "current_tab": "userbot",
        "settings": config_db_service.get_all_settings(),
        "userbot_status": await get_userbot_status(),
    }
    return templates.TemplateResponse("settings_userbot.html", context)

@admin_router.get("/settings/payment", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def settings_payment(request: Request):
    """Payment settings page."""
    current_user = request.state.current_user
    if not current_user:
        return RedirectResponse(url="/auth")

    # Get payment statistics
    payment_stats = {
        "total_users": db_service.get_user_count(),
        "active_subscribers": db_service.get_active_subscriber_count(),
        "pending_payments": db_service.get_pending_payment_count(),
        "total_revenue": db_service.get_total_revenue(),
    }

    context = {
        "request": request,
        "current_user": current_user,
        "current_tab": "payment",
        "settings": config_db_service.get_all_settings(),
        "payment_stats": payment_stats,
    }
    return templates.TemplateResponse("settings_payment.html", context)

@admin_router.get("/legacy-admin-panel", response_class=HTMLResponse, dependencies=[Depends(verify_jwt)])
async def admin_panel_legacy(request: Request):
    """Legacy admin panel route for backwards compatibility."""
    current_user = request.state.current_user
    if not current_user:
        return RedirectResponse(url="/auth")

    context = {
        "request": request,
        "current_user": current_user,
        "settings": config_db_service.get_all_settings(),
        "admins": config_db_service.get_all_admins(),
        "logs": config_db_service.get_audit_logs(100),
        "messages": request.session.pop("_messages", []),
        "userbot_status": await get_userbot_status(),
    }
    return templates.TemplateResponse("admin.html", context)

@auth_router.post("/auth/login")
async def admin_login(request: Request, username: str = Form(...), password: str = Form(...), next: str = Form("/admin")):
    if config_db_service.check_admin_password(username, password):
        response = RedirectResponse(url=next.replace("/admin", "/"), status_code=status.HTTP_303_SEE_OTHER)
        create_jwt_cookie(response, username)
        return response
    else:
        return templates.TemplateResponse("auth.html", {
            "request": request,
            "error": "Noto'g'ri foydalanuvchi nomi yoki parol",
            "next": next.replace("/admin", "/"),
        }, status_code=401)

@auth_router.get("/auth/logout")
async def admin_logout(request: Request):
    response = RedirectResponse(url="/auth", status_code=status.HTTP_303_SEE_OTHER)
    # Clear both potential cookies for safety
    try:
        response.delete_cookie(ADMIN_JWT_COOKIE_NAME)
    except Exception:
        pass
    try:
        response.delete_cookie("admin_session")
    except Exception:
        pass
    return response

# API ENDPOINTS FOR SETTINGS

@api_router.post("/settings/telegram")
async def api_update_telegram_settings(request: Request):
    """API endpoint to update telegram settings."""
    current_user = "api"
    try:
        form_data = await request.form()
        settings_dict = dict(form_data)
        
        # Validate telegram-specific settings
        telegram_settings = {
            "private_channel_id": settings_dict.get("private_channel_id", "").strip(),
            "group_id": settings_dict.get("group_id", "").strip(),
            "ai_confirmations_topic_id": settings_dict.get("ai_confirmations_topic_id", "").strip(),
            "confirmation_topic_id": settings_dict.get("confirmation_topic_id", "").strip(),
            "realtime_orders_topic_id": settings_dict.get("realtime_orders_topic_id", "").strip(),
            "find_orders_topic_id": settings_dict.get("find_orders_topic_id", "").strip(),
            "default_ai_model": settings_dict.get("default_ai_model", "gemini-2.5-flash-lite")
        }
        
        success = config_db_service.update_settings(telegram_settings, current_user)
        if success:
            config.load_from_db()  # Reload config into memory
            return JSONResponse({"success": True, "message": "Telegram settings updated successfully"})
        else:
            return JSONResponse({"success": False, "message": "Failed to update telegram settings"})
            
    except Exception as e:
        logger.error(f"Error updating telegram settings: {e}")
        return JSONResponse({"success": False, "message": str(e)})

# New API endpoints for Telegram Admin Card Management
@api_router.get("/telegram-admins")
async def get_telegram_admins():
    """Get list of Telegram admins as JSON."""
    try:
        from src.services.admin_config_service import admin_config_service
        admins = admin_config_service.get_admins()
        logger.info(f"Retrieved admins: {admins}")
        return {"success": True, "admins": admins}
    except Exception as e:
        logger.error(f"Error getting telegram admins: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)}
        )

@api_router.post("/telegram-admins")
async def add_telegram_admin(request: Request):
    """Add new Telegram admin."""
    try:
        form = await request.form()
        admin_id = form.get("admin_id", "").strip()
        admin_name = form.get("admin_name", "").strip()

        if not admin_id or not admin_name:
            return JSONResponse(status_code=400, content={"success": False, "message": "Admin ID va nomi talab qilinadi"})

        if not admin_id.isdigit():
            return JSONResponse(status_code=400, content={"success": False, "message": "Admin ID raqamdan iborat bo'lishi kerak"})

        from src.services.admin_config_service import admin_config_service
        success = admin_config_service.add_admin(admin_id, admin_name)

        if success:
            config.load_from_db()  # Refresh in-memory config
            return {"success": True, "message": "Admin muvaffaqiyatli qo'shildi"}
        else:
            return JSONResponse(status_code=400, content={"success": False, "message": "Bu admin ID allaqachon mavjud"})

    except Exception as e:
        logger.error(f"Error adding telegram admin: {e}")
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})

@api_router.put("/telegram-admins/{admin_id}")
async def update_telegram_admin(request: Request, admin_id: str):
    """Update existing Telegram admin."""
    try:
        form = await request.form()
        new_name = form.get("admin_name", "").strip()

        if not new_name:
            return JSONResponse(status_code=400, content={"success": False, "message": "Admin nomi talab qilinadi"})

        from src.services.admin_config_service import admin_config_service
        success = admin_config_service.update_admin(admin_id, new_name)

        if success:
            config.load_from_db()  # Refresh in-memory config
            return {"success": True, "message": "Admin muvaffaqiyatli yangilandi"}
        else:
            return JSONResponse(status_code=404, content={"success": False, "message": "Admin topilmadi"})

    except Exception as e:
        logger.error(f"Error updating telegram admin {admin_id}: {e}")
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})

@api_router.delete("/telegram-admins/{admin_id}")
async def delete_telegram_admin(request: Request, admin_id: str):
    """
    Delete a Telegram admin by their ID.
    """
    try:
        from src.services.admin_config_service import admin_config_service

        # Check if there's only one admin left (optional restriction - can be removed if no admins should be allowed)
        current_admins = admin_config_service.get_admins()
        if len(current_admins) <= 1:
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "Kamida bitta admin bo'lishi kerak"}
            )

        success = admin_config_service.delete_admin(admin_id)

        if success:
            config.load_from_db()  # Refresh in-memory config
            return {"success": True, "message": "Admin muvaffaqiyatli o'chirildi"}
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "message": "Admin topilmadi"}
            )

    except Exception as e:
        logger.error(f"Error deleting admin {admin_id}: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)}
        )

@api_router.post("/settings/userbot")
async def api_update_userbot_settings(request: Request):
    """API endpoint to update userbot settings."""
    current_user = "api"
    try:
        form_data = await request.form()
        settings_dict = dict(form_data)
        
        # Validate and save userbot settings
        userbot_settings = {
            "userbot_phone_number": settings_dict.get("userbot_phone_number", "").strip(),
            "userbot_password": settings_dict.get("userbot_password", "").strip(),
        }
        
        success = config_db_service.update_settings(userbot_settings, current_user)
        if success:
            config.load_from_db()  # Reload config into memory
            return JSONResponse({"success": True, "message": "Userbot settings updated successfully"})
        else:
            return JSONResponse({"success": False, "message": "Failed to update userbot settings"})
            
    except Exception as e:
        logger.error(f"Error updating userbot settings: {e}")
        return JSONResponse({"success": False, "message": str(e)})

@api_router.post("/settings/payment")
async def api_update_payment_settings(request: Request):
    """API endpoint to update payment settings."""
    current_user = "api"
    try:
        form_data = await request.form()
        settings_dict = dict(form_data)
        
        # Only allow timing-related payment settings from the web.
        # Subscription price is managed exclusively via the bot.
        payment_settings = {
            "payment_checking_wait_time": settings_dict.get("payment_checking_wait_time", "60"),
            "order_delay_time": settings_dict.get("order_delay_time", "1"),
            # Force-enable AI payment confirmation (no toggle in UI)
            "auto_payment_confirmation": "true",
        }

        success = config_db_service.update_settings(payment_settings, current_user)
        if success:
            config.load_from_db()  # Reload config into memory
            return JSONResponse({"success": True, "message": "Payment settings updated successfully"})
        else:
            return JSONResponse({"success": False, "message": "Failed to update payment settings"})
            
    except Exception as e:
        logger.error(f"Error updating payment settings: {e}")
        return JSONResponse({"success": False, "message": str(e)})

def _clean_userbot_session_files():
    """Clean up all userbot session files to prevent conflicts."""
    try:
        base = _get_userbot_session_path()
        # Common SQLite session file patterns
        patterns = [
            base,
            base + ".session",
            base + ".session-journal",
            base + ".session-shm",
            base + ".session-wal",
        ]

        # Also check for any leftover temporary files in the directory
        session_dir = os.path.dirname(base)
        if os.path.exists(session_dir):
            import glob
            temp_patterns = [
                os.path.join(session_dir, "*.session*"),
                os.path.join(session_dir, "*.tmp"),
                os.path.join(session_dir, "*.temp"),
            ]
            for pattern in temp_patterns:
                for temp_file in glob.glob(pattern):
                    patterns.append(temp_file)

        cleaned_count = 0
        for f in patterns:
            try:
                if os.path.exists(f):
                    os.remove(f)
                    cleaned_count += 1
                    logger.info(f"Cleaned up session file: {f}")
            except Exception as e:
                logger.warning(f"Failed to delete session file {f}: {e}")

        logger.info(f"Session cleanup completed. Removed {cleaned_count} files.")
        return cleaned_count > 0

    except Exception as e:
        logger.error(f"Error during session cleanup: {e}")
        return False

@api_router.post("/userbot/send-code")
async def api_userbot_send_code(request: Request):
    """API endpoint to send verification code to userbot phone."""
    global userbot_client, userbot_login_state, userbot_run_task
    try:
        # Check current status to prevent conflicts
        status = await get_userbot_status()
        if status.get("authorized"):
            return JSONResponse({
                "success": False,
                "message": "Userbot is already connected and authorized",
                "status": status
            })

        # Parse and validate form data
        form = await request.form()
        phone = (form.get('userbot_phone_number') or '').strip()
        password = form.get('userbot_password') or ''

        # Fallback to saved settings if fields not provided
        settings = config_db_service.get_all_settings()
        phone = phone or settings.get('userbot_phone_number') or ''
        password = password or settings.get('userbot_password') or ''

        # Enhanced validation
        app_id_int = config.API_ID
        api_hash = config.API_HASH
        if not isinstance(app_id_int, int) or not api_hash:
            return JSONResponse({
                "success": False,
                "message": "APP_ID and API_HASH must be configured in .env file"
            })

        if not phone:
            return JSONResponse({
                "success": False,
                "message": "Phone number is required"
            })

        if not phone.startswith('+') or len(phone) < 10:
            return JSONResponse({
                "success": False,
                "message": "Phone number must include country code (e.g. +1234567890)"
            })

        # Save settings for future reuse
        config_db_service.update_settings({
            'userbot_phone_number': phone,
            'userbot_password': password,
        }, "api")

        # Perform complete cleanup of existing connections
        await _cleanup_existing_userbot_connections()

        # Initialize login state
        userbot_login_state.clear()
        userbot_login_state.update({
            'state': 'SENDING_CODE',
            'phone': phone,
            'password': password,
            'app_id': app_id_int,
            'api_hash': api_hash,
            'started_at': datetime.datetime.now().isoformat()
        })

        # Create and configure new client
        client = await _create_fresh_userbot_client(app_id_int, api_hash)

        try:
            # Send verification code
            await client.send_code_request(phone)

            # Update login state with client and success
            userbot_login_state.update({
                'client': client,
                'state': 'AWAITING_CODE',
                'code_sent_at': datetime.datetime.now().isoformat()
            })

            logger.info(f"Verification code sent successfully to {phone}")
            return JSONResponse({
                "success": True,
                "message": "Verification code sent successfully",
                "state": "AWAITING_CODE"
            })

        except Exception as e:
            # Clean up client on failure
            try:
                await client.disconnect()
            except Exception:
                pass
            userbot_login_state.clear()

            # Handle specific Telegram errors
            error_message = str(e)
            if "phone number" in error_message.lower():
                return JSONResponse({
                    "success": False,
                    "message": "Invalid phone number format"
                })
            elif "flood" in error_message.lower():
                return JSONResponse({
                    "success": False,
                    "message": "Too many requests. Please wait before trying again"
                })
            elif "authorization key" in error_message.lower() and "ip addresses" in error_message.lower():
                # Session conflict - attempt one retry after cleanup
                logger.warning("Session conflict detected, attempting retry...")
                _clean_userbot_session_files()
                await asyncio.sleep(1)

                try:
                    retry_client = await _create_fresh_userbot_client(app_id_int, api_hash)
                    await retry_client.send_code_request(phone)

                    userbot_login_state.update({
                        'client': retry_client,
                        'state': 'AWAITING_CODE',
                        'code_sent_at': datetime.datetime.now().isoformat()
                    })

                    return JSONResponse({
                        "success": True,
                        "message": "Verification code sent successfully (after session cleanup)",
                        "state": "AWAITING_CODE"
                    })
                except Exception as retry_e:
                    try:
                        await retry_client.disconnect()
                    except Exception:
                        pass
                    userbot_login_state.clear()
                    return JSONResponse({
                        "success": False,
                        "message": f"Failed to send code after retry: {str(retry_e)}"
                    })
            else:
                return JSONResponse({
                    "success": False,
                    "message": f"Failed to send verification code: {error_message}"
                })

    except Exception as e:
        logger.error(f"Error sending userbot verification code: {e}")
        userbot_login_state.clear()
        return JSONResponse({"success": False, "message": f"Internal error: {str(e)}"})


async def _cleanup_existing_userbot_connections():
    """Clean up any existing userbot connections and state."""
    global userbot_client, userbot_run_task, userbot_login_state

    # Cancel running task
    try:
        if userbot_run_task and not userbot_run_task.done():
            userbot_run_task.cancel()
            await asyncio.sleep(0.1)  # Give task time to cancel
    except Exception as e:
        logger.debug(f"Error canceling userbot task: {e}")
    finally:
        userbot_run_task = None

    # Disconnect main client
    try:
        if userbot_client:
            await userbot_client.disconnect()
    except Exception as e:
        logger.debug(f"Error disconnecting main userbot client: {e}")
    finally:
        userbot_client = None

    # Disconnect any temporary login client
    try:
        tmp_client = userbot_login_state.get('client')
        if tmp_client:
            await tmp_client.disconnect()
    except Exception as e:
        logger.debug(f"Error disconnecting temp login client: {e}")

    # Clear login state
    userbot_login_state.clear()

    # Clean session files
    _clean_userbot_session_files()

    # Wait for cleanup to complete
    await asyncio.sleep(0.5)


async def _create_fresh_userbot_client(app_id: int, api_hash: str) -> TelegramClient:
    """Create a fresh TelegramClient with proper configuration."""
    client = TelegramClient(
        _get_userbot_session_path(),
        app_id,
        api_hash,
        system_version="4.16.30-vxCUSTOM",
        use_ipv6=False
    )

    await client.connect()

    # Ensure client is not already authorized (shouldn't be after cleanup)
    if await client.is_user_authorized():
        logger.warning("Client was already authorized after cleanup, logging out...")
        await client.log_out()
        await client.disconnect()

        # Clean up again and recreate
        _clean_userbot_session_files()
        await asyncio.sleep(0.5)

        client = TelegramClient(
            _get_userbot_session_path(),
            app_id,
            api_hash,
            system_version="4.16.30-vxCUSTOM",
            use_ipv6=False
        )
        await client.connect()

    return client


async def _run_userbot_with_protection(client: TelegramClient, phone: str):
    """
    Protected wrapper for userbot.run_until_disconnected() with crash detection and logging.
    This prevents silent crashes from killing the userbot connection.
    """
    global userbot_client, userbot_run_task

    try:
        logger.info(f"🛡️ Starting protected userbot task for {phone}")

        # Verify client is still valid before starting
        if not client or not client.is_connected():
            logger.error(f"❌ Client is not connected when starting protected task for {phone}")
            return

        # Run the main event loop
        await client.run_until_disconnected()

        # If we reach here, the task completed normally (user logout)
        logger.info(f"✅ Userbot task completed normally for {phone}")

    except asyncio.CancelledError:
        # Task was cancelled (normal during logout)
        logger.info(f"🛑 Userbot task cancelled for {phone}")
        raise  # Re-raise to complete cancellation

    except Exception as e:
        # Unexpected crash - this is the bug we're looking for!
        logger.critical(f"💥 USERBOT TASK CRASHED for {phone}: {e}", exc_info=True)

        # Log additional context
        try:
            is_connected = client.is_connected() if client else False
            is_authorized = (await client.is_user_authorized()) if client else False
            logger.critical(f"📊 Client state at crash for {phone}: connected={is_connected}, authorized={is_authorized}")
        except Exception as status_error:
            logger.error(f"❌ Failed to get client status during crash handling for {phone}: {status_error}")

        # Clear the global state since the task crashed
        if userbot_client == client:
            logger.warning(f"🧹 Clearing global userbot_client due to crash for {phone}")
            userbot_client = None

        if userbot_run_task and not userbot_run_task.done():
            logger.warning(f"🧹 Clearing global userbot_run_task due to crash for {phone}")
            userbot_run_task = None

        # Try to disconnect the client cleanly
        try:
            if client:
                await client.disconnect()
                logger.info(f"🔌 Disconnected crashed client for {phone}")
        except Exception as disconnect_error:
            logger.error(f"❌ Failed to disconnect crashed client for {phone}: {disconnect_error}")

    finally:
        logger.info(f"🏁 Protected userbot task finished for {phone}")


@api_router.post("/userbot/verify-code")
async def api_userbot_verify_code(request: Request):
    """API endpoint to verify userbot code and complete authentication."""
    global userbot_client, userbot_login_state, userbot_run_task
    try:
        # Parse form data
        form_data = await request.form()
        code = form_data.get('code', '').strip()

        # Validate code
        if not code:
            return JSONResponse({"success": False, "message": "Verification code is required"})

        if len(code) != 5 or not code.isdigit():
            return JSONResponse({"success": False, "message": "Code must be exactly 5 digits"})

        # Check login state
        if not userbot_login_state:
            return JSONResponse({
                "success": False,
                "message": "No active login session. Please send code first"
            })

        client: Optional[TelegramClient] = userbot_login_state.get('client')
        phone: str = userbot_login_state.get('phone', '')
        password: str = userbot_login_state.get('password', '')
        login_state = userbot_login_state.get('state', '')

        if not client or not phone:
            userbot_login_state.clear()
            return JSONResponse({
                "success": False,
                "message": "Invalid login session. Please start login process again"
            })

        if login_state != 'AWAITING_CODE':
            return JSONResponse({
                "success": False,
                "message": f"Invalid login state: {login_state}. Please start login process again"
            })

        # Update login state
        userbot_login_state['state'] = 'VERIFYING_CODE'

        try:
            # Attempt sign in with code
            logger.info(f"🔐 Attempting sign in for {phone} with code {code[:2]}***")

            try:
                await client.sign_in(phone=phone, code=code)
                logger.info(f"✅ Successfully signed in with code for {phone}")

            except SessionPasswordNeededError:
                logger.info(f"🔑 2FA password required for {phone}")
                # 2FA required
                if not password:
                    userbot_login_state['state'] = 'AWAITING_PASSWORD'
                    logger.warning(f"❌ 2FA password not provided for {phone}")
                    return JSONResponse({
                        "success": False,
                        "message": "Two-factor authentication password is required. Please set password in settings and try again",
                        "requires_password": True
                    })

                # Try with 2FA password
                try:
                    logger.info(f"🔑 Attempting 2FA sign in for {phone} with a password of length {len(password)}")
                except Exception:
                    logger.info(f"🔑 Attempting 2FA sign in for {phone} (password length unavailable)")
                userbot_login_state['state'] = 'VERIFYING_PASSWORD'
                await client.sign_in(password=password)
                logger.info(f"✅ Successfully signed in with 2FA password for {phone}")

            except PhoneCodeInvalidError:
                logger.warning(f"❌ Invalid verification code for {phone}: {code}")
                userbot_login_state['state'] = 'AWAITING_CODE'  # Reset to allow retry
                return JSONResponse({
                    "success": False,
                    "message": "Invalid verification code. Please check and try again"
                })

            except Exception as signin_error:
                error_msg = str(signin_error)
                logger.error(f"❌ Sign-in failed for {phone}: {error_msg}")
                userbot_login_state['state'] = 'ERROR'

                # Handle specific errors
                if "phone code invalid" in error_msg.lower():
                    userbot_login_state['state'] = 'AWAITING_CODE'
                    return JSONResponse({
                        "success": False,
                        "message": "Invalid verification code"
                    })
                elif "password invalid" in error_msg.lower() or "hash value you entered is invalid" in error_msg.lower():
                    # Specific flag to help UI prompt for updating password
                    return JSONResponse({
                        "success": False,
                        "message": "Login failed: The 2FA password is incorrect.",
                        "invalid_password": True,
                        "requires_password": True
                    })
                elif "authorization key" in error_msg.lower():
                    # Session conflict
                    logger.error(f"🔥 Session conflict detected for {phone}")
                    await client.disconnect()
                    userbot_login_state.clear()
                    return JSONResponse({
                        "success": False,
                        "message": "Session conflict detected. Please clean session and try again"
                    })
                else:
                    return JSONResponse({
                        "success": False,
                        "message": f"Authentication failed: {error_msg}"
                    })

            # Successful authentication - set up userbot
            logger.info(f"🚀 Setting up userbot for {phone}")
            userbot_login_state['state'] = 'FINALIZING'

            # Verify client is still connected and authorized
            try:
                is_connected = client.is_connected()
                is_authorized = await client.is_user_authorized()
                logger.info(f"📊 Pre-setup status for {phone}: connected={is_connected}, authorized={is_authorized}")

                if not is_connected or not is_authorized:
                    logger.error(f"❌ Client lost connection/auth immediately after sign-in for {phone}")
                    return JSONResponse({
                        "success": False,
                        "message": "Connection lost immediately after authentication. Please try again."
                    })
            except Exception as e:
                logger.error(f"❌ Failed to verify client status for {phone}: {e}")
                return JSONResponse({
                    "success": False,
                    "message": f"Failed to verify connection: {str(e)}"
                })

            # Attach event handlers
            logger.info(f"🎧 Attaching event handlers for {phone}")
            _attach_userbot_handlers(client)

            # Set userbot client in processor
            logger.info(f"🔗 Setting userbot in message processor for {phone}")
            message_processor.set_userbot(client)

            # Promote temp client to main client
            logger.info(f"⬆️ Promoting temp client to main client for {phone}")
            userbot_client = client

            # Start the client's main loop with protection
            if userbot_run_task is None or userbot_run_task.done():
                logger.info(f"🏃 Starting protected run_until_disconnected task for {phone}")
                userbot_run_task = asyncio.create_task(_run_userbot_with_protection(userbot_client, phone))

            # Get user info for response
            try:
                me = await userbot_client.get_me()
                user_info = {
                    "id": me.id,
                    "first_name": me.first_name,
                    "last_name": me.last_name,
                    "username": me.username,
                    "phone": me.phone or phone
                }
                logger.info(f"👤 Retrieved user info for {phone}: {user_info['first_name']} ({user_info['phone']})")
            except Exception as e:
                logger.warning(f"⚠️ Failed to get user info after successful login for {phone}: {e}")
                user_info = {"phone": phone}

            # Verify final status
            try:
                final_status = await get_userbot_status()
                logger.info(f"📊 Final status check for {phone}: {final_status['state']} (connected={final_status['connected']}, authorized={final_status['authorized']})")
            except Exception as e:
                logger.warning(f"⚠️ Failed final status check for {phone}: {e}")

            # Clear login state
            userbot_login_state.clear()

            logger.info(f"🎉 Userbot successfully connected and authenticated for {phone}")
            return JSONResponse({
                "success": True,
                "message": "Userbot connected and authenticated successfully",
                "user": user_info,
                "state": "AUTHORIZED"
            })

        except Exception as e:
            # Clean up on any error
            try:
                await client.disconnect()
            except Exception:
                pass
            userbot_login_state.clear()

            logger.error(f"Error during code verification: {e}")
            return JSONResponse({
                "success": False,
                "message": f"Verification failed: {str(e)}"
            })

    except Exception as e:
        # Clean up login state on any unexpected error
        userbot_login_state.clear()
        logger.error(f"Error verifying userbot code: {e}")
        return JSONResponse({"success": False, "message": f"Internal error: {str(e)}"})

@api_router.post("/userbot/logout")
async def api_userbot_logout(request: Request):
    """API endpoint to completely logout and clean userbot session."""
    global userbot_client, userbot_run_task, userbot_login_state
    logout_results = {
        "task_cancelled": False,
        "server_logout": False,
        "client_disconnected": False,
        "session_files_cleaned": False,
        "temp_client_cleaned": False
    }

    try:
        logger.info("Starting complete userbot logout process...")

        # Step 1: Cancel the running task
        try:
            if userbot_run_task and not userbot_run_task.done():
                userbot_run_task.cancel()
                # Wait for cancellation
                try:
                    await asyncio.wait_for(userbot_run_task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
                logout_results["task_cancelled"] = True
                logger.info("✓ Userbot task cancelled")
        except Exception as e:
            logger.warning(f"Error cancelling userbot task: {e}")
        finally:
            userbot_run_task = None

        # Step 2: Logout from Telegram servers and disconnect main client
        if userbot_client:
            try:
                # First try to revoke the authorization key on the server
                if userbot_client.is_connected():
                    try:
                        await userbot_client.log_out()
                        logout_results["server_logout"] = True
                        logger.info("✓ Userbot logged out from Telegram servers")
                    except Exception as e:
                        logger.warning(f"Failed to logout from server (will continue with disconnect): {e}")

                # Then disconnect the client
                try:
                    await userbot_client.disconnect()
                    logout_results["client_disconnected"] = True
                    logger.info("✓ Userbot client disconnected")
                except Exception as e:
                    logger.warning(f"Error disconnecting userbot client: {e}")

            except Exception as e:
                logger.warning(f"Error during main client logout: {e}")
            finally:
                userbot_client = None

        # Step 3: Clean up any temporary login client
        try:
            tmp_client = userbot_login_state.get('client')
            if tmp_client:
                try:
                    if tmp_client.is_connected():
                        await tmp_client.disconnect()
                    logout_results["temp_client_cleaned"] = True
                    logger.info("✓ Temporary login client cleaned")
                except Exception as e:
                    logger.warning(f"Error cleaning temp client: {e}")
        except Exception as e:
            logger.warning(f"Error accessing temp client: {e}")

        # Step 4: Clear login state
        userbot_login_state.clear()

        # Step 5: Clean up all session files
        try:
            cleaned_files = _clean_userbot_session_files()
            logout_results["session_files_cleaned"] = cleaned_files
            if cleaned_files:
                logger.info("✓ Session files cleaned")
            else:
                logger.info("✓ No session files to clean")
        except Exception as e:
            logger.warning(f"Error cleaning session files: {e}")

        # Step 6: Clear userbot reference in message processor
        try:
            message_processor.set_userbot(None)
            logger.info("✓ Userbot reference cleared from message processor")
        except Exception as e:
            logger.warning(f"Error clearing userbot from message processor: {e}")

        # Determine success based on critical operations
        critical_success = (
            logout_results["task_cancelled"] or userbot_run_task is None
        ) and userbot_client is None

        if critical_success:
            logger.info("🎉 Userbot logout completed successfully")
            return JSONResponse({
                "success": True,
                "message": "Userbot logged out and cleaned successfully",
                "details": logout_results
            })
        else:
            logger.warning("Userbot logout completed with some issues")
            return JSONResponse({
                "success": True,
                "message": "Userbot logout completed (with some warnings - check logs)",
                "details": logout_results
            })

    except Exception as e:
        logger.error(f"Critical error during userbot logout: {e}")

        # Emergency cleanup - ensure everything is reset
        try:
            userbot_client = None
            userbot_run_task = None
            userbot_login_state.clear()
            _clean_userbot_session_files()
        except Exception:
            pass

        return JSONResponse({
            "success": False,
            "message": f"Logout failed: {str(e)}",
            "details": logout_results
        })

@api_router.post("/userbot/clean-session")
async def api_userbot_clean_session(request: Request):
    """API endpoint to forcefully clean all userbot session data."""
    global userbot_client, userbot_run_task, userbot_login_state
    cleanup_results = {
        "task_force_cancelled": False,
        "client_force_disconnected": False,
        "temp_client_cleaned": False,
        "session_files_cleaned": False,
        "state_cleared": False
    }

    try:
        logger.info("🧹 Manual session cleanup requested - performing force cleanup")

        # Step 1: Force cancel any running task
        try:
            if userbot_run_task and not userbot_run_task.done():
                userbot_run_task.cancel()
                # Don't wait for graceful cancellation in force cleanup
                cleanup_results["task_force_cancelled"] = True
                logger.info("✓ Userbot task force cancelled")
        except Exception as e:
            logger.warning(f"Error force cancelling task: {e}")
        finally:
            userbot_run_task = None

        # Step 2: Force disconnect main client without logout
        try:
            if userbot_client:
                try:
                    await userbot_client.disconnect()
                    cleanup_results["client_force_disconnected"] = True
                    logger.info("✓ Main userbot client force disconnected")
                except Exception as e:
                    logger.warning(f"Error force disconnecting main client: {e}")
        except Exception as e:
            logger.warning(f"Error accessing main client: {e}")
        finally:
            userbot_client = None

        # Step 3: Clean any temporary login client
        try:
            tmp_client = userbot_login_state.get('client')
            if tmp_client:
                try:
                    await tmp_client.disconnect()
                    cleanup_results["temp_client_cleaned"] = True
                    logger.info("✓ Temporary login client cleaned")
                except Exception as e:
                    logger.warning(f"Error cleaning temp login client: {e}")
        except Exception as e:
            logger.warning(f"Error accessing temp login client: {e}")

        # Step 4: Clear all state
        try:
            userbot_login_state.clear()
            cleanup_results["state_cleared"] = True
            logger.info("✓ All userbot state cleared")
        except Exception as e:
            logger.warning(f"Error clearing state: {e}")

        # Step 5: Force clean all session files
        try:
            cleaned_files = _clean_userbot_session_files()
            cleanup_results["session_files_cleaned"] = cleaned_files
            if cleaned_files:
                logger.info(f"✓ {cleaned_files} session files force cleaned")
            else:
                logger.info("✓ No session files found to clean")
        except Exception as e:
            logger.warning(f"Error force cleaning session files: {e}")

        # Step 6: Clear userbot from message processor
        try:
            message_processor.set_userbot(None)
            logger.info("✓ Userbot reference cleared from message processor")
        except Exception as e:
            logger.warning(f"Error clearing userbot from processor: {e}")

        # Force cleanup is always considered successful if we can clear the critical state
        success = userbot_client is None and userbot_run_task is None

        if success:
            logger.info("🎉 Force cleanup completed successfully - userbot is now fully reset")
            return JSONResponse({
                "success": True,
                "message": "Session force cleaned successfully - userbot fully reset",
                "details": cleanup_results
            })
        else:
            logger.warning("Force cleanup completed with some issues")
            return JSONResponse({
                "success": True,
                "message": "Session cleanup completed (check logs for warnings)",
                "details": cleanup_results
            })

    except Exception as e:
        logger.error(f"Critical error during session force cleanup: {e}")

        # Ultimate emergency reset
        try:
            userbot_client = None
            userbot_run_task = None
            userbot_login_state.clear()
        except Exception:
            pass

        return JSONResponse({
            "success": False,
            "message": f"Force cleanup failed: {str(e)}",
            "details": cleanup_results
        })

@api_router.get("/userbot/status")
async def api_userbot_status(request: Request):
    """API endpoint to get userbot status."""
    try:
        status = await get_userbot_status()

        # Try to create the response and catch serialization errors
        try:
            response = JSONResponse({"success": True, "status": status})
            return response
        except TypeError as json_error:
            logger.error(f"🚨 JSON SERIALIZATION ERROR: {json_error}")
            logger.error(f"🚨 Status object that failed to serialize: {type(status)}")

            # Try to identify the problematic part
            import json
            try:
                json.dumps(status)
            except TypeError as e:
                logger.error(f"🚨 JSON dumps failed: {e}")

            # Return a safe error response
            return JSONResponse({
                "success": False,
                "message": f"Status serialization failed: {str(json_error)}",
                "error_type": "JSON_SERIALIZATION_ERROR"
            })

    except Exception as e:
        logger.error(f"Error getting userbot status: {e}", exc_info=True)
        return JSONResponse({"success": False, "message": str(e)})

@admin_router.post("/settings", dependencies=[Depends(verify_jwt)])
async def update_settings_legacy(request: Request):
    """Legacy settings update endpoint - redirects to telegram settings."""
    return RedirectResponse(url="/settings", status_code=303)


# Legacy userbot endpoints - kept for backwards compatibility but redirect to new settings

@admin_router.post("/userbot/login", dependencies=[Depends(verify_jwt)])
async def userbot_login_legacy(request: Request):
    """Legacy userbot login endpoint - redirects to userbot settings."""
    return RedirectResponse(url="/settings/userbot", status_code=303)

@admin_router.post("/userbot/submit-code", dependencies=[Depends(verify_jwt)])
async def userbot_submit_code_legacy(request: Request, code: str = Form(...)):
    """Legacy userbot code submission endpoint - redirects to userbot settings."""
    return RedirectResponse(url="/settings/userbot", status_code=303)

@admin_router.post("/userbot/logout", dependencies=[Depends(verify_jwt)])
async def userbot_logout_legacy(request: Request):
    """Legacy userbot logout endpoint - redirects to userbot settings."""
    return RedirectResponse(url="/settings/userbot", status_code=303)

@admin_router.post("/settings/admins/add", dependencies=[Depends(verify_jwt)])
async def add_admin(request: Request, username: str = Form(...), password: str = Form(...), role: str = Form(...)):
    current_user = request.state.current_user
    if not current_user:
        return RedirectResponse(url="/auth", status_code=303)
    
    if not password or len(password) < 6:
        flash(request, "Parol kamida 6 belgidan iborat bo'lishi kerak.", "danger")
        return RedirectResponse(url="/settings", status_code=303)

    admin_id = config_db_service.add_admin(username, password, role, current_user)
    if admin_id:
        flash(request, f"'{username}' nomli admin qo'shildi.", "success")
    else:
        flash(request, f"'{username}' nomli admin allaqachon mavjud.", "danger")
    return RedirectResponse(url="/settings", status_code=303)

@admin_router.get("/settings/admins/delete/{admin_id}", dependencies=[Depends(verify_jwt)])
async def delete_admin(request: Request, admin_id: int):
    current_user = request.state.current_user
    if not current_user:
        return RedirectResponse(url="/auth", status_code=303)
    
    config_db_service.delete_admin(admin_id, current_user)
    flash(request, f"#{admin_id} IDli admin o'chirildi.", "success")
    return RedirectResponse(url="/settings", status_code=303)

@admin_router.get("/generate-thumbnails", dependencies=[Depends(verify_jwt)])
async def generate_thumbnails(request: Request):
    """Manually trigger thumbnail generation for all videos."""
    current_user = request.state.current_user
    if not current_user:
        return RedirectResponse(url="/auth", status_code=303)
    
    try:
        from processor import generate_thumbnails_for_all_videos
        generate_thumbnails_for_all_videos()
        flash(request, "Video miniatyurlari muvaffaqiyatli yaratildi!", "success")
    except Exception as e:
        flash(request, f"Xatolik: {str(e)}", "danger")
    
    return RedirectResponse(url="/settings", status_code=303)

@admin_app.get("/uploads/{filepath:path}")
@user_app.get("/uploads/{filepath:path}")
async def serve_upload_file(filepath: str):
    """Serve uploaded files including subdirectories like thumbnails."""
    file_path = os.path.join("uploads", filepath)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return FileResponse(file_path)
    else:
        raise HTTPException(status_code=404, detail="File not found") 

@admin_app.get("/favicon.ico", include_in_schema=False)
@user_app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Serve favicon to prevent 404 errors."""
    # Return a simple response or redirect to a default favicon
    return Response(status_code=204)

@admin_app.get("/404", response_class=HTMLResponse)
async def not_found_page(request: Request):
    """Explicit 404 page route."""
    return templates.TemplateResponse("404.html", {"request": request, "current_user": request.state.current_user}, status_code=404)


# Public API endpoints (for admin panel AJAX)
@admin_app.get("/api/orders")
async def get_orders_api():
    """API endpoint to get orders data for real-time updates."""
    try:
        orders = db_service.get_all_orders_with_details(limit=100)
        
        # Add status styling and formatting
        for order in orders:
            order['file_count'] = order.get('file_count', 0)
            # Format collection status
            if order['collection_status'] == 'open':
                order['status_class'] = 'success'
                order['status_text'] = 'OCHIQ'
                order['status_icon'] = '🟢'
            elif order['collection_status'] == 'close':
                order['status_class'] = 'warning'
                order['status_text'] = 'YOPIQ'
                order['status_icon'] = '🔴'
            elif order['collection_status'] == 'finish':
                order['status_class'] = 'primary'
                order['status_text'] = 'YAKUNLANGAN'
                order['status_icon'] = '✅'
            else:
                order['status_class'] = 'secondary'
                order['status_text'] = 'NOMA\'LUM'
                order['status_icon'] = '⚪'
            
            # Format user name
            user_name = []
            if order.get('name'):
                user_name.append(order['name'])
            if order.get('surname'):
                user_name.append(order['surname'])
            order['user_full_name'] = ' '.join(user_name) if user_name else 'N/A'

            # Format user code
            if order.get('code') and order.get('collection_id'):
                order['user_code'] = f"{order['collection_id']}-{order['code']}"
            else:
                order['user_code'] = ''

            # Format order date if available
            if order.get('collection_created_at'):
                order['order_date_formatted'] = order['collection_created_at'][:10]
            else:
                order['order_date_formatted'] = 'N/A'
        
        return {
            "orders": orders,
            "count": len(orders),
            "timestamp": "now"
        }
    except Exception as e:
        return {"error": str(e)}

@admin_app.get("/api/orders/{order_id}")
async def get_order_details_api(order_id: int):
    """API endpoint to get detailed information for a single order."""
    order_details = db_service.get_order_details(order_id)
    if not order_details:
        raise HTTPException(status_code=404, detail="Order not found")
    
    return order_details

@admin_app.get("/api/user/{user_code}/orders")
async def get_user_orders_by_collection_admin(user_code: str, collection_id: int):
    """Get orders for a specific user in a specific collection (Admin API)."""
    try:
        # Get user by code
        user = db_service.get_user_by_code(user_code)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get orders for this user and collection using efficient DB method
        orders = db_service.get_user_orders_by_collection(user['id'], collection_id, limit=100)
        
        # Filter out cancelled orders (status != 1)
        active_orders = []
        for order in orders:
            if order.get('status', 1) == 1:
                active_orders.append(order)
        
        return {
            "success": True,
            "orders": active_orders,
            "count": len(active_orders)
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting user orders by collection (admin): {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@admin_app.get("/api/users")
async def get_users_api():
    """API endpoint to get users data."""
    try:
        users = db_service.get_all_users_with_stats()
        
        # Add user status info and formatting
        for user in users:
            # Ensure plain user_code for API consumers
            user['user_code'] = user.get('code') or ''
            # Format registration status
            if user['reg_step'] == 'done':
                user['reg_status'] = 'Completed'
                user['reg_status_class'] = 'success'
                user['reg_status_icon'] = '✅'
            else:
                user['reg_status'] = f'Step: {user["reg_step"].title()}'
                user['reg_status_class'] = 'warning'
                user['reg_status_icon'] = '⏳'
            
            # Format active status
            user['active_status'] = 'Active' if user['is_active'] else 'Inactive'
            user['active_status_class'] = 'success' if user['is_active'] else 'secondary'
            user['active_status_icon'] = '✅' if user['is_active'] else '⚪'
            
            # Format latest order date
            if user['latest_order_date']:
                user['latest_order_formatted'] = user['latest_order_date'][:10]
            else:
                user['latest_order_formatted'] = 'No orders'
        
        return {
            "users": users,
            "count": len(users),
            "timestamp": "now"
        }
    except Exception as e:
        return {"error": str(e)}


@admin_app.get("/debug/orders")
async def debug_orders():
    """Debug endpoint to check order image paths."""
    try:
        conn = db_service._get_connection()
        cursor = conn.cursor()
        
        # Get some recent orders with their images
        cursor.execute("""
            SELECT o.id, oi.image_url 
            FROM orders o
            LEFT JOIN order_files oi ON o.id = oi.order_id
            ORDER BY o.id DESC
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        debug_info = []
        for row in results:
            debug_info.append({
                "order_id": row[0], 
                "image_url": row[1]
            })
        
        return {"debug_info": debug_info}
    except Exception as e:
        return {"error": str(e)}


# Include routers BEFORE defining the catch-all route
admin_app.include_router(auth_router)
admin_app.include_router(api_router)
admin_app.include_router(admin_api_router)
admin_app.include_router(admin_router)

user_app.include_router(user_router)
user_app.include_router(user_api_router)

# Catch-all route for any unmatched paths (must be last)
@admin_app.get("/{path:path}", response_class=HTMLResponse)
async def catch_all(request: Request, path: str):
    """Catch all unmatched routes and show 404 page."""
    raise HTTPException(status_code=404, detail="Sahifa topilmadi")

@user_app.get("/{path:path}", response_class=HTMLResponse)
async def user_catch_all(request: Request, path: str):
    """Catch all for user app."""
    raise HTTPException(status_code=404, detail="Foydalanuvchi sahifasi topilmadi")

if __name__ == "__main__":
    # Initialize databases
    config_db_service.initialize_database()
    db_service.initialize_database()
    
    # Run the admin web server on configured ADMIN_PORT (default 4040) for development
    port = int(os.getenv('ADMIN_PORT', '4040'))
    uvicorn.run("web_app:admin_app", host="0.0.0.0", port=port, reload=True)
