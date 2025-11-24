"""
Alert System for COVID-19 Data Quality Monitoring

This module handles sending alerts when data validation fails.
Supports email and Slack notifications.

Author: Data Engineering Team
Date: November 2025
"""

import os
import logging
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dotenv import load_dotenv
import requests


# Load environment variables
load_dotenv()


class AlertSystem:
    """Class to handle alerts for data quality failures."""
    
    def __init__(self, config):
        """
        Initialize the AlertSystem.
        
        Args:
            config (dict): Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.alert_config = config.get('alerts', {})
        
    def send_alert(self, validation_results, failed_expectations):
        """
        Send alerts based on validation results.
        
        Args:
            validation_results (dict): Great Expectations validation results
            failed_expectations (list): List of failed expectation descriptions
            
        Returns:
            bool: True if at least one alert was sent successfully
        """
        if not self.alert_config.get('enabled', False):
            self.logger.info("Alerts are disabled in configuration")
            return False
            
        alert_sent = False
        message = self._create_alert_message(validation_results, failed_expectations)
        
        # Send email alert
        if self.alert_config.get('email', {}).get('enabled', False):
            if self._send_email_alert(message):
                alert_sent = True
                
        # Send Slack alert
        if self.alert_config.get('slack', {}).get('enabled', False):
            if self._send_slack_alert(message):
                alert_sent = True
                
        return alert_sent
        
    def _create_alert_message(self, validation_results, failed_expectations):
        """
        Create formatted alert message.
        
        Args:
            validation_results (dict): Great Expectations validation results
            failed_expectations (list): List of failed expectation descriptions
            
        Returns:
            dict: Message dictionary with subject and body
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        success = validation_results.get('success', False)
        
        subject = f"COVID-19 Data Validation {'PASSED' if success else 'FAILED'} - {timestamp}"
        
        body = f"""
COVID-19 Data Quality Monitoring Alert
{'=' * 60}

Timestamp: {timestamp}
Validation Status: {'SUCCESS' if success else 'FAILURE'}

"""
        
        if not success and failed_expectations:
            body += f"Failed Expectations ({len(failed_expectations)}):\n"
            body += "-" * 60 + "\n"
            for i, expectation in enumerate(failed_expectations, 1):
                body += f"{i}. {expectation}\n"
            body += "\n"
            
        # Add statistics if available
        statistics = validation_results.get('statistics', {})
        if statistics:
            body += "Validation Statistics:\n"
            body += "-" * 60 + "\n"
            body += f"Total Expectations: {statistics.get('evaluated_expectations', 'N/A')}\n"
            body += f"Successful: {statistics.get('successful_expectations', 'N/A')}\n"
            body += f"Failed: {statistics.get('unsuccessful_expectations', 'N/A')}\n"
            body += f"Success Percentage: {statistics.get('success_percent', 'N/A')}%\n\n"
            
        body += """
Action Required:
- Review the failed expectations above
- Check the quarantined data in data/quarantine/
- Review Great Expectations Data Docs for detailed results
- Fix data quality issues before re-running the pipeline

"""
        
        return {
            'subject': subject,
            'body': body
        }
        
    def _send_email_alert(self, message):
        """
        Send email alert.
        
        Args:
            message (dict): Message dictionary with subject and body
            
        Returns:
            bool: True if email sent successfully
        """
        email_config = self.alert_config.get('email', {})
        
        try:
            # Get email credentials
            smtp_server = email_config.get('smtp_server')
            smtp_port = email_config.get('smtp_port')
            sender = email_config.get('sender')
            recipients = email_config.get('recipients', [])
            password_env = email_config.get('password_env_var', 'EMAIL_PASSWORD')
            password = os.getenv(password_env)
            
            if not all([smtp_server, smtp_port, sender, recipients, password]):
                self.logger.warning("Email configuration incomplete, skipping email alert")
                return False
                
            # Create message
            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = message['subject']
            msg.attach(MIMEText(message['body'], 'plain'))
            
            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender, password)
                server.send_message(msg)
                
            self.logger.info(f"Email alert sent to {', '.join(recipients)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {e}")
            return False
            
    def _send_slack_alert(self, message):
        """
        Send Slack alert.
        
        Args:
            message (dict): Message dictionary with subject and body
            
        Returns:
            bool: True if Slack message sent successfully
        """
        slack_config = self.alert_config.get('slack', {})
        
        try:
            webhook_env = slack_config.get('webhook_url_env_var', 'SLACK_WEBHOOK_URL')
            webhook_url = os.getenv(webhook_env)
            
            if not webhook_url:
                self.logger.warning("Slack webhook URL not configured, skipping Slack alert")
                return False
                
            # Format message for Slack
            slack_message = {
                "text": message['subject'],
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": message['subject']
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"```{message['body']}```"
                        }
                    }
                ]
            }
            
            # Send to Slack
            response = requests.post(
                webhook_url,
                json=slack_message,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            response.raise_for_status()
            
            self.logger.info("Slack alert sent successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send Slack alert: {e}")
            return False


def test_alerts(config):
    """
    Test function to verify alert system configuration.
    
    Args:
        config (dict): Configuration dictionary
    """
    print("Testing Alert System Configuration")
    print("=" * 60)
    
    alert_system = AlertSystem(config)
    
    # Create test validation results
    test_results = {
        'success': False,
        'statistics': {
            'evaluated_expectations': 10,
            'successful_expectations': 7,
            'unsuccessful_expectations': 3,
            'success_percent': 70.0
        }
    }
    
    test_failures = [
        "expect_column_values_to_not_be_null (column: new_cases)",
        "expect_column_values_to_be_between (column: new_deaths, min: 0)",
        "expect_column_values_to_be_unique (columns: location, date)"
    ]
    
    # Send test alert
    success = alert_system.send_alert(test_results, test_failures)
    
    if success:
        print("\nTest alert sent successfully!")
    else:
        print("\nFailed to send test alert. Check configuration and logs.")


if __name__ == "__main__":
    import yaml
    import sys
    
    # Load configuration
    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run test
    test_alerts(config)
