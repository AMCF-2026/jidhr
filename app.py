"""
Jidhr - AMCF Operations Assistant
=================================
Main Flask application.

This is the entry point - all the logic lives in assistant.py and clients/
"""

from flask import Flask, render_template, request, jsonify
from config import Config
from assistant import get_assistant

# Initialize Flask
app = Flask(__name__)
app.secret_key = Config.SECRET_KEY


# =============================================================================
# ROUTES
# =============================================================================

@app.route('/')
def home():
    """Render the chat interface"""
    return render_template('chat.html')


@app.route('/chat', methods=['POST'])
def chat():
    """Process a chat message"""
    try:
        data = request.get_json()
        message = data.get('message', '').strip()
        
        if not message:
            return jsonify({"error": "No message provided"}), 400
        
        # Get assistant and process query
        assistant = get_assistant()
        response = assistant.process_query(message)
        
        return jsonify({"response": response})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/clear', methods=['POST'])
def clear():
    """Clear conversation history"""
    assistant = get_assistant()
    assistant.clear_history()
    return jsonify({"status": "cleared"})


@app.route('/health')
def health():
    """Health check endpoint for Railway"""
    # Check for required config
    missing = Config.validate()
    if missing:
        return jsonify({
            "status": "unhealthy",
            "missing_config": missing
        }), 500
    
    return jsonify({"status": "healthy", "service": "jidhr"})


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    # Validate config on startup
    missing = Config.validate()
    if missing:
        print(f"⚠️  Missing environment variables: {', '.join(missing)}")
        print("   Set these in your .env file or Railway dashboard")
    
    print("🌳 Starting Jidhr - AMCF Operations Assistant")
    app.run(
        host='0.0.0.0',
        port=Config.PORT,
        debug=Config.DEBUG
    )
