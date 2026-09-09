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
from authlib.integrations.base_client.errors import (
    MismatchingStateError,
    OAuthError,
)
from config import Config
from clients.users import get_or_create_user, get_user_by_id

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
    """A signed-in user, backed by a row in the `users` table.

    `id` is the users.id primary key, not the email address. Flask-Login
    serialises it into the session cookie and hands it back as a string, so
    load_user() converts it once on the way in.
    """

    def __init__(self, id, email, display_name=None, role="staff",
                 is_active=True, picture=None, csuite_profile_id=None):
        self.id = int(id)
        self.email = email
        self.display_name = display_name or (email or "").split("@")[0]
        self.role = role
        # Carried so app.py can build an Actor without a second query. Stored
        # as a string: it is an opaque identifier, never arithmetic.
        self.csuite_profile_id = (
            str(csuite_profile_id) if csuite_profile_id is not None else None
        )
        self.picture = picture
        self._is_active = bool(is_active)

    @classmethod
    def from_row(cls, row, picture=None):
        """Build a User from a `users` table row."""
        return cls(
            id=row["id"],
            email=row["email"],
            display_name=row.get("display_name"),
            role=row.get("role") or "staff",
            is_active=row.get("is_active", True),
            picture=picture,
            csuite_profile_id=row.get("csuite_profile_id"),
        )

    @property
    def is_active(self):
        """Flask-Login checks this; a deactivated user cannot log in."""
        return self._is_active

    @property
    def name(self):
        """Template-facing alias. chat.html renders `user.name`."""
        return self.display_name

    def __repr__(self):
        return f"<User {self.id} {self.email} role={self.role}>"


# =============================================================================
# FLASK-LOGIN SETUP
# =============================================================================

@login_manager.user_loader
def load_user(user_id):
    """Load a user by users.id for Flask-Login.

    Read from the database on every request rather than a per-worker cache:
    that is what makes deactivating someone take effect immediately instead
    of whenever their gunicorn worker happens to recycle.

    Returns None — which Flask-Login treats as "not logged in" — for an
    unknown id, a deactivated user, or a database failure. Failing closed is
    the right default for an auth path.
    """
    if not user_id:
        return None

    try:
        row = get_user_by_id(int(user_id))
    except (TypeError, ValueError):
        logger.warning("Malformed user id in session cookie: %r", user_id)
        return None
    except Exception as e:
        logger.error(f"Could not load user {user_id}: {e}", exc_info=True)
        return None

    if row is None:
        logger.info("Session referenced a user that no longer exists: %s", user_id)
        return None

    if not row.get("is_active", True):
        logger.warning("Rejected session for deactivated user: %s", row.get("email"))
        return None

    return User.from_row(row)


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

    except MismatchingStateError:
        # Google fired a second callback — user is already logged in
        # from the first one.  Redirect silently without touching the
        # session so we don't corrupt the valid login.
        logger.info("Duplicate OAuth callback (MismatchingStateError) — redirecting silently")
        return redirect(url_for('home'))

    except OAuthError as e:
        logger.error(f"OAuth error: {e}", exc_info=True)
        return redirect(url_for('auth.login', error='Authentication failed. Please try again.'))

    try:
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

        # Record the login and read back the row that decides access.
        try:
            row = get_or_create_user(email, name)
        except Exception as e:
            logger.exception(f"Could not record login for {email}: {e}")
            return redirect(url_for(
                'auth.login',
                error='Sign-in is temporarily unavailable. Please try again.'))

        if row is None:
            logger.error(f"No user row returned for {email}")
            return redirect(url_for(
                'auth.login',
                error='Sign-in is temporarily unavailable. Please try again.'))

        # A valid Google account is not the same as an active Jidhr account.
        if not row.get("is_active", True):
            logger.warning(f"Access denied - deactivated account: {email}")
            return redirect(url_for(
                'auth.login',
                error='This account has been deactivated. Contact an administrator.'))

        user = User.from_row(row, picture=picture)

        # remember=False: no long-lived "remember me" cookie. Access lasts as
        # long as the session cookie and no longer.
        login_user(user, remember=False)

        # PERMANENT_SESSION_LIFETIME only applies to permanent sessions.
        # Without this the cookie is a browser-session cookie with no expiry
        # at all, and the configured 12-hour limit never fires.
        session.permanent = True

        logger.info(f"Login successful: {email} (id={user.id}, role={user.role})")

        # Redirect to originally requested page or home
        next_page = session.pop('next', None)
        return redirect(next_page or url_for('home'))

    except Exception as e:
        logger.exception(f"OAuth callback error: {e}")
        return redirect(url_for('auth.login', error='Authentication failed. Please try again.'))


# Per-request assistant state that lives in the session cookie. Logging out
# must drop it: the next person to sign in on this browser would otherwise
# inherit a half-finished draft, and its contents are not theirs to see.
SESSION_STATE_KEYS = ("draft_state", "workflow_state")


@auth_bp.route('/logout')
def logout():
    """Log out current user"""
    if current_user.is_authenticated:
        logger.info(f"Logout: {current_user.email}")

    for key in SESSION_STATE_KEYS:
        session.pop(key, None)

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