import re
from fastapi import HTTPException


PASSWORD_PATTERN = re.compile(r"^(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,128}$")


def validate_password(password: str) -> None:
    if not PASSWORD_PATTERN.match(password or ""):
        raise HTTPException(
            status_code=400,
            detail=(
                "Password must be 8-128 characters and include at least one uppercase letter, "
                "one number, and one special character."
            ),
        )
