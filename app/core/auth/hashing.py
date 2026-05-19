"""
Password Hashing
────────────────
Native bcrypt wrapper. No passlib dependency required.

Public API (backward-compatible):
    Hash.bcrypt(password)               → hashed string
    Hash.verify(hashed, plain)          → bool
    Hash.checkpw(plain, hashed)         → bool  (alias for verify)
"""

import bcrypt


class Hash:
    """Provides static methods for password hashing and verification using bcrypt."""

    @staticmethod
    def bcrypt(password: str) -> str:
        """Hashes a plaintext password and returns the bcrypt hash as a string."""
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    @staticmethod
    def verify(hashed_password: str, plain_password: str) -> bool:
        """Returns True if plain_password matches the stored bcrypt hash."""
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            hashed_password.encode("utf-8"),
        )

    @staticmethod
    def checkpw(plain_password: str, hashed_password: str) -> bool:
        """Alias for verify() — argument order matches the original passlib API."""
        return Hash.verify(hashed_password, plain_password)