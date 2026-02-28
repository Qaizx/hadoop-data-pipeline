# jobs/utils/alerts.py
import os
import smtplib
from email.mime.text import MIMEText


def send_email_alert(subject: str, body: str):
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")

    if not smtp_user or not smtp_password:
        print("   ⚠️  SMTP not configured, skipping email alert")
        return

    msg = MIMEText(f"<pre>{body}</pre>", "html")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, smtp_user, msg.as_string())
        print(f"   📧 Alert email sent")
    except Exception as e:
        print(f"   ⚠️  Email failed: {e}")


def send_quality_alert(filepath: str, report: str):
    subject = f"❌ [ETL] Data Quality Failed: {filepath.split('/')[-1]}"
    send_email_alert(subject, report)