"""
Jidhr - AMCF Operations Assistant
=================================
Main Flask application.

This is the entry point - all the logic lives in assistant.py and clients/
"""

import logging
import sys
from flask import Flask, render_template, request, jsonify
from flask_login import login_required, current_user
from werkzeug.middleware.proxy_fix import ProxyFix
from config import Config
from assistant import get_assistant
from auth import init_auth

# =============================================================================
# LOGGING SETUP
# =============================================================================

# Configure logging to stdout (Railway captures this)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def log_user_action(action, details=""):
    """Log an action with the current user's email"""
    user_email = current_user.email if current_user.is_authenticated else "anonymous"
    if details:
        logger.info(f"[{user_email}] {action}: {details}")
    else:
        logger.info(f"[{user_email}] {action}")


# =============================================================================
# FLASK APP
# =============================================================================

app = Flask(__name__)
app.secret_key = Config.SECRET_KEY

# Trust proxy headers (Railway terminates SSL)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Initialize authentication
init_auth(app)


# =============================================================================
# ROUTES
# =============================================================================

@app.route('/')
@login_required
def home():
    """Render the chat interface"""
    log_user_action("Accessed chat interface")
    return render_template('chat.html', user=current_user)


@app.route('/chat', methods=['POST'])
@login_required
def chat():
    """Process a chat message"""
    try:
        data = request.get_json()
        message = data.get('message', '').strip()
        
        if not message:
            logger.warning("Empty message received")
            return jsonify({"error": "No message provided"}), 400
        
        log_user_action("Chat request", message[:100] + "..." if len(message) > 100 else message)
        
        # Get assistant and process query
        assistant = get_assistant()
        response = assistant.process_query(message)
        
        log_user_action("Chat response", response[:100] + "..." if len(response) > 100 else response)
        
        return jsonify({"response": response})
    
    except Exception as e:
        logger.error(f"Chat error: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/clear', methods=['POST'])
@login_required
def clear():
    """Clear conversation history"""
    log_user_action("Cleared conversation")
    assistant = get_assistant()
    assistant.clear_history()
    return jsonify({"status": "cleared"})


@app.route('/health')
def health():
    """Health check endpoint for Railway (no auth required)"""
    # Check for required config
    missing = Config.validate()
    if missing:
        logger.warning(f"Health check failed - missing: {missing}")
        return jsonify({
            "status": "unhealthy",
            "missing_config": missing
        }), 500
    
    logger.debug("Health check passed")
    return jsonify({"status": "healthy", "service": "jidhr"})


# =============================================================================
# STARTUP
# =============================================================================

# Log startup info
logger.info("=" * 60)
logger.info("🌳 Jidhr - AMCF Operations Assistant")
logger.info("=" * 60)

missing = Config.validate()
if missing:
    logger.warning(f"Missing environment variables: {', '.join(missing)}")
else:
    logger.info("✅ All environment variables configured")

logger.info(f"Claude Model: {Config.CLAUDE_MODEL}")
logger.info(f"CSuite URL: {Config.CSUITE_BASE_URL}")
logger.info(f"Auth Domain: @{Config.ALLOWED_DOMAIN}")
logger.info("=" * 60)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    logger.info(f"Starting development server on port {Config.PORT}")
    app.run(
        host='0.0.0.0',
        port=Config.PORT,
        debug=Config.DEBUG
    )
