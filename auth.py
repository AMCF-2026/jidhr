"""
Jidhr Authentication
====================
Google OAuth authentication for AMCF staff.
"""

import logging
from functools import wraps
from flask import Blueprint, redirect, url_for, session, flash, request, render_template
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user
from authlib.integrations.flask_client import OAuth
from config import Config

logger = logging.getLogger(__name__)

# =============================================================================
# BLUEPRINT & EXTENSIONS
# =============================================================================

auth_bp = Blueprint('auth', __name__)
login_manager = LoginManager()
oauth = OAuth()


# =============================================================================
# USER MODEL
# =============================================================================

class User(UserMixin):
    """Simple user class for Flask-Login"""
    
    def __init__(self, email, name=None, picture=None):
        self.id = email  # Flask-Login requires an id attribute
        self.email = email
        self.name = name or email.split('@')[0]
        self.picture = picture
    
    def __repr__(self):
        return f"<User {self.email}>"


# In-memory user store (sufficient for small team, session-based)
_users = {}


def get_or_create_user(email, name=None, picture=None):
    """Get existing user or create new one"""
    if email not in _users:
        _users[email] = User(email, name, picture)
        logger.info(f"New user created: {email}")
    return _users[email]


# =============================================================================
# FLASK-LOGIN SETUP
# =============================================================================

@login_manager.user_loader
def load_user(user_id):
    """Load user by ID (email) for Flask-Login"""
    return _users.get(user_id)


@login_manager.unauthorized_handler
def unauthorized():
    """Redirect unauthorized users to login"""
    return redirect(url_for('auth.login'))


# =============================================================================
# OAUTH SETUP
# =============================================================================

def init_oauth(app):
    """Initialize OAuth with Google provider"""
    oauth.init_app(app)
    
    oauth.register(
        name='google',
        client_id=Config.GOOGLE_CLIENT_ID,
        client_secret=Config.GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile'
        }
    )


# =============================================================================
# ROUTES
# =============================================================================

@auth_bp.route('/login')
def login():
    """Show login page"""
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    
    error = request.args.get('error')
    return render_template('login.html', error=error)


@auth_bp.route('/login/google')
def login_google():
    """Initiate Google OAuth flow"""
    redirect_uri = url_for('auth.callback', _external=True)
    logger.info(f"Starting OAuth flow, redirect_uri: {redirect_uri}")
    return oauth.google.authorize_redirect(redirect_uri)


@auth_bp.route('/auth/callback')
def callback():
    """Handle Google OAuth callback"""
    try:
        # Get token from Google
        token = oauth.google.authorize_access_token()
        
        # Get user info
        user_info = token.get('userinfo')
        if not user_info:
            user_info = oauth.google.userinfo()
        
        email = user_info.get('email', '').lower()
        name = user_info.get('name')
        picture = user_info.get('picture')
        
        logger.info(f"OAuth callback for: {email}")
        
        # Validate domain
        if not email.endswith(f'@{Config.ALLOWED_DOMAIN}'):
            logger.warning(f"Access denied - invalid domain: {email}")
            return redirect(url_for('auth.login', error=f'Access restricted to @{Config.ALLOWED_DOMAIN} accounts'))
        
        # Create/get user and log them in
        user = get_or_create_user(email, name, picture)
        login_user(user, remember=True)
        
        logger.info(f"Login successful: {email}")
        
        # Redirect to originally requested page or home
        next_page = session.pop('next', None)
        return redirect(next_page or url_for('home'))
    
    except Exception as e:
        logger.error(f"OAuth callback error: {str(e)}", exc_info=True)
        return redirect(url_for('auth.login', error='Authentication failed. Please try again.'))


@auth_bp.route('/logout')
def logout():
    """Log out current user"""
    if current_user.is_authenticated:
        logger.info(f"Logout: {current_user.email}")
    logout_user()
    return redirect(url_for('auth.login'))


# =============================================================================
# INITIALIZATION
# =============================================================================

def init_auth(app):
    """Initialize authentication for Flask app"""
    # Initialize Flask-Login
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    
    # Initialize OAuth
    init_oauth(app)
    
    # Register blueprint
    app.register_blueprint(auth_bp)
    
    logger.info(f"Auth initialized - domain restriction: @{Config.ALLOWED_DOMAIN}")