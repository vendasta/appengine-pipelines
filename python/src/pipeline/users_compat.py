"""Compatibility layer for App Engine Users API.

This module provides a drop-in replacement for google.appengine.api.users
by parsing App Engine request headers directly, which are still available
on App Engine Standard even without the bundled services library.

On App Engine, when a user is authenticated, the following headers are set:
- X-Appengine-User-Email: The user's email address
- X-Appengine-User-Id: A unique user ID
- X-Appengine-User-Is-Admin: "1" if user is an admin, "0" otherwise
"""

from flask import request
from typing import Optional


class User:
    """Represents an authenticated user."""

    def __init__(self, email: Optional[str] = None, user_id: Optional[str] = None):
        """Initialize a User.

        Args:
            email: User's email address
            user_id: Unique user identifier
        """
        self._email = email
        self._user_id = user_id

    def email(self) -> Optional[str]:
        """Return the user's email address."""
        return self._email

    def user_id(self) -> Optional[str]:
        """Return the user's unique ID."""
        return self._user_id

    def nickname(self) -> Optional[str]:
        """Return a nickname for the user (email before @)."""
        if self._email:
            return self._email.split('@')[0]
        return None

    def __str__(self):
        return str(self._email)

    def __repr__(self):
        return f"User(email={self._email!r}, user_id={self._user_id!r})"


def get_current_user() -> Optional[User]:
    """Get the currently logged-in user.

    Returns:
        User object if a user is logged in, None otherwise
    """
    # Check for App Engine user headers
    email = request.headers.get('X-Appengine-User-Email')
    user_id = request.headers.get('X-Appengine-User-Id')

    if email:
        return User(email=email, user_id=user_id)

    return None


def is_current_user_admin() -> bool:
    """Check if the current user is an administrator.

    Returns:
        True if the current user is an admin, False otherwise
    """
    # Check the admin header
    is_admin = request.headers.get('X-Appengine-User-Is-Admin', '0')
    return is_admin == '1'


def create_login_url(dest_url: str) -> str:
    """Create a login URL.

    On App Engine, the login URL is handled by the runtime. We return
    a URL to the App Engine login handler with the destination URL.

    Args:
        dest_url: URL to redirect to after login

    Returns:
        Login URL
    """
    import urllib.parse
    # App Engine provides a built-in login handler
    return f"/_ah/login?continue={urllib.parse.quote(dest_url)}"


def create_logout_url(dest_url: str) -> str:
    """Create a logout URL.

    Args:
        dest_url: URL to redirect to after logout

    Returns:
        Logout URL
    """
    import urllib.parse
    # App Engine provides a built-in logout handler
    return f"/_ah/logout?continue={urllib.parse.quote(dest_url)}"
