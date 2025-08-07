"""
Notification utilities for sending alerts and messages.
Another common module for dependency analysis testing.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def send_alert(message: str, severity: str = "INFO", recipients: Optional[list] = None) -> bool:
    """
    Send alert message to specified recipients.
    
    Args:
        message: Alert message content
        severity: Alert severity level (INFO, WARNING, ERROR, CRITICAL)
        recipients: List of recipient email addresses
        
    Returns:
        Boolean indicating success
    """
    try:
        # Simulate alert sending
        logger.info(f"[{severity}] Alert: {message}")
        
        if recipients:
            logger.info(f"Sending to recipients: {recipients}")
        
        # Simulate different outcomes based on message content
        if "failed" in message.lower():
            # High priority for failure messages
            logger.warning(f"High priority alert sent: {message}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to send alert: {str(e)}")
        return False

class SlackNotifier:
    """Slack notification utility."""
    
    def __init__(self, webhook_url: str, channel: str = "#alerts"):
        self.webhook_url = webhook_url
        self.channel = channel
    
    def send_message(self, message: str, username: str = "Airflow Bot") -> bool:
        """Send message to Slack channel."""
        try:
            # Simulate Slack API call
            logger.info(f"Sending Slack message to {self.channel}: {message}")
            return True
        except Exception as e:
            logger.error(f"Slack notification failed: {str(e)}")
            return False