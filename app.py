"""
Jidhr - AMCF Operations Assistant
=================================
Main Flask application.

This is the entry point - all the logic lives in assistant.py and clients/
"""

import logging
import sys
from flask import Flask, render_template, request, jsonify
from config import Config
from assistant import get_assistant

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

# =============================================================================
# FLASK APP
# =============================================================================

app = Flask(__name__)
app.secret_key = Config.SECRET_KEY


# =============================================================================
# ROUTES
# =============================================================================

@app.route('/')
def home():
    """Render the chat interface"""
    logger.info("Home page accessed")
    return render_template('chat.html')


@app.route('/chat', methods=['POST'])
def chat():
    """Process a chat message"""
    try:
        data = request.get_json()
        message = data.get('message', '').strip()
        
        if not message:
            logger.warning("Empty message received")
            return jsonify({"error": "No message provided"}), 400
        
        logger.info(f"Chat request: {message[:100]}...")
        
        # Get assistant and process query
        assistant = get_assistant()
        response = assistant.process_query(message)
        
        logger.info(f"Chat response: {response[:100]}...")
        
        return jsonify({"response": response})
    
    except Exception as e:
        logger.error(f"Chat error: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/clear', methods=['POST'])
def clear():
    """Clear conversation history"""
    logger.info("Conversation cleared")
    assistant = get_assistant()
    assistant.clear_history()
    return jsonify({"status": "cleared"})


@app.route('/health')
def health():
    """Health check endpoint for Railway"""
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
