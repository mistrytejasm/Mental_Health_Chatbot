import os
import logging
import random
import string
import smtplib
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Optional
from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Gmail SMTP Configuration
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD", "")
SENDER_NAME = "MindBuddy"


def generate_otp() -> str:
    """Generate a 6-digit OTP."""
    return str(random.randint(100000, 999999))


def validate_email(email: str) -> bool:
    """
    Basic email validation using regex pattern.
    Returns True if email format is valid, False otherwise.
    """
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    is_valid = re.match(pattern, email) is not None
    
    if not is_valid:
        logger.warning(f"Invalid email format: {email}")
    return is_valid


async def send_email(recipient: str, subject: str, body: str) -> bool:
    """
    Send email using Gmail SMTP (completely free).
    Returns True if successful, False otherwise.
    
    Requirements:
    - Set SENDER_EMAIL and SENDER_PASSWORD (.env file)
    - SENDER_PASSWORD should be a Gmail App Password (not your regular Gmail password)
    """
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        logger.error("Gmail credentials not configured. Please add SENDER_EMAIL and SENDER_PASSWORD to .env")
        return False
    
    if not validate_email(recipient):
        logger.error(f"Invalid recipient email: {recipient}")
        return False
    
    try:
        # Run SMTP in async context to prevent blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            _send_email_sync,
            recipient,
            subject,
            body
        )
        return result
    except Exception as e:
        logger.error(f"Unexpected error while sending email to {recipient}: {e}")
        return False


def _send_email_sync(recipient: str, subject: str, body: str) -> bool:
    """
    Synchronous helper function to send email using Gmail SMTP.
    This is called from the async send_email function.
    """
    try:
        # Create email message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        msg["To"] = recipient
        
        # Add HTML body
        msg.attach(MIMEText(body, "html"))
        
        # Send via Gmail SMTP
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        logger.info(f"Email sent successfully to {recipient}")
        return True
    
    except smtplib.SMTPAuthenticationError:
        logger.error(f"Gmail authentication failed. Check SENDER_EMAIL and SENDER_PASSWORD (.env)")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while sending email to {recipient}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error while sending email to {recipient}: {e}")
        return False


async def send_otp_email(recipient: str, otp: str) -> bool:
    """Send OTP email for password reset."""
    settings = get_settings()
    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #f5f5f5; padding: 20px; border-radius: 8px;">
            <h1 style="color: #333; text-align: center;">Password Reset Request</h1>
            
            <p style="color: #666; font-size: 16px;">
                Hi,
            </p>
            
            <p style="color: #666; font-size: 16px; line-height: 1.6;">
                You requested to reset your password for your MindBuddy account. 
                Please use the OTP below to proceed. This OTP is valid for {settings.OTP_EXPIRY_MINUTES} minutes.
            </p>
            
            <div style="background-color: #0c2340; padding: 20px; border-radius: 8px; text-align: center; margin: 30px 0;">
                <h2 style="color: white; font-size: 32px; letter-spacing: 5px; margin: 0;">
                    {otp}
                </h2>
            </div>
            
            <p style="color: #666; font-size: 14px;">
                Please do not share this OTP with anyone. If you did not request a password reset, 
                please ignore this email.
            </p>
            
            <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">
            
            <p style="color: #999; font-size: 12px;">
                Regards,<br>
                MindBuddy Team
            </p>
        </div>
    </div>
    """
    
    return await send_email(recipient, "Password Reset OTP - MindBuddy", html_body)
