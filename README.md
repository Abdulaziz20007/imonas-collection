# Order Management System

A comprehensive order tracking system with both Telegram bot integration and web interface.

## Features

### Telegram Bot

- User registration and management
- Photo submission for orders
- Collection management (open/close/finish)
- Admin panel with inline keyboards
- Real-time order notifications
- User search by unique codes

### Web Interface

- Collections overview with statistics
- User management per collection
- Order details with image galleries
- Mobile-friendly responsive design
- Click-to-expand order details
- Image modal preview

## Quick Start

### Prerequisites

- Python 3.8+
- Telegram Bot Token
- Virtual environment (recommended)

### Installation

1. **Clone and Setup**

   ```bash
   git clone <repository>
   cd telegram
   python -m venv venv

   # On Windows
   venv\Scripts\activate

   # On Unix/macOS
   source venv/bin/activate
   ```

2. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Configuration**
   Create a `.env` file in the project root:
   ```env
   TELEGRAM_BOT_TOKEN=your_bot_token_here
   ADMIN_ID=your_telegram_user_id
   GROUP_ID=your_group_id_for_notifications
   FIND_ORDERS_TOPIC_ID=topic_id_for_order_search
   REALTIME_ORDERS_TOPIC_ID=topic_id_for_real_time_notifications
   DATABASE_FILE=database.db
   ```

### Running the Application

**Option 1: Run Both Services (Recommended)**

```bash
python app.py
```

This starts:

- 🤖 Telegram Bot (polling)
- 🌐 Web Interface (WEB_INTERFACE_URL)

**Option 2: Run Services Separately**

```bash
# Bot only
python -c "from app import setup_bot_application; app = setup_bot_application(); app.run_polling()"

# Web only
python web_app.py
```

**Option 3: Using Startup Script**

```bash
python start_web.py  # Web interface only
```

## Web Interface Usage

1. **Collections Page** (`http://localhost:3030`)

   - View all collections with status indicators
   - See user counts and order statistics
   - Click on collections to view details

2. **Collection Detail Page** (`http://localhost:3030/collection/{id}`)
   - View users who have orders in the collection
   - Click on users to expand their order details
   - View order images in a gallery format
   - Click on images for full-size preview

## Telegram Bot Usage

### For Users

- `/start` - Register and get your unique code
- `/mystats` - View your order statistics
- Send photos to create orders
- Enter series numbers when prompted

### For Admins

- **🆕 Yangi kolleksiya** - Create new collection
- **📀 Aktiv kolleksiyani ko'rish** - View active collection
- **📋 Oxirgi 10 ta kolleksiya** - List recent collections
- **🔍 Mahsulotlarni qidirish** - Search orders by user code

## Database Schema

The system uses SQLite with the following main tables:

- `users` - User registration and profile data
- `collections` - Collection management (open/close/finish)
- `orders` - Order records linking users to collections
- `order_images` - Image storage for orders
- `code` - Unique codes for users per collection

## File Structure

```
telegram/
├── app.py                 # Main application (bot + web)
├── web_app.py            # Web interface only
├── start_web.py          # Web startup script
├── database.py           # Database service layer
├── processor.py          # Message processing logic
├── config.py             # Configuration management
├── requirements.txt      # Python dependencies
├── templates/            # HTML templates
│   ├── base.html        # Base template with responsive design
│   ├── collections.html # Collections list page
│   └── collection_detail.html # Collection detail page
├── uploads/              # Image storage directory
└── static/               # Static assets (auto-created)
```

## Development

### Adding New Features

1. Database changes: Update `database.py` and schema
2. Bot features: Add handlers in `app.py` and logic in `processor.py`
3. Web features: Add routes in `web_app.py` and templates

### Environment Variables

- `TELEGRAM_BOT_TOKEN` - Required for bot functionality
- `ADMIN_ID` - Telegram user ID for admin access
- `GROUP_ID` - Group for notifications
- `FIND_ORDERS_TOPIC_ID` - Topic for order search
- `REALTIME_ORDERS_TOPIC_ID` - Topic for real-time notifications
- `DATABASE_FILE` - SQLite database file path

## Deployment

For production deployment:

1. Set up a proper web server (nginx + gunicorn)
2. Use environment variables for sensitive configuration
3. Set up proper logging and monitoring
4. Consider using PostgreSQL instead of SQLite
5. Implement SSL/TLS for the web interface

## Troubleshooting

### Common Issues

1. **Port 3030 already in use**: Stop existing processes or change port
2. **Database errors**: Check file permissions and initialization
3. **Bot not responding**: Verify token and network connectivity
4. **Images not loading**: Check uploads directory permissions

### Logs

The application provides detailed logging for both bot and web components. Check console output for error messages and debugging information.

## License

This project is licensed under the MIT License.
