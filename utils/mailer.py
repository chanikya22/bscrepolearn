"""
Mailer that sends email via Microsoft Graph API, built directly around the
same auth/request pattern as the known-working send_test_email script
(same get_token() function, same sendMail URL/headers/payload shape) -
just wrapped in the original Mailer class interface with image/attachment
support layered on top.

Public interface is unchanged from the previous SMTP-based Mailer:
    Mailer.send_email(subject, body, image_paths, attachment_file_path,
                       recipient_emails, cc_emails)

Required environment variables (.env):
    GRAPH_TENANT_ID
    GRAPH_CLIENT_ID
    GRAPH_CLIENT_SECRET
    MAIL_SENDER_MAILBOX   - mailbox to send FROM

pip install msal requests python-dotenv
"""

import os
import uuid
import base64
import mimetypes
from typing import List, Optional
import environmentconfig

import msal
import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("GRAPH_TENANT_ID")
CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
MAILBOX = os.getenv("MAIL_SENDER_MAILBOX")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]


def get_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)

    if "access_token" not in result:
        raise Exception(
            f"Token acquisition failed: {result.get('error')} - "
            f"{result.get('error_description')}"
        )
    return result["access_token"]


class Mailer:
    @staticmethod
    def send_email(subject: str, body: str, image_paths: List[str],
                    attachment_file_path: Optional[str], recipient_emails: List[str],
                    cc_emails: List[str]) -> None:
        """
        Send an email with optional embedded images and one attachment via
        Microsoft Graph, sending as MAILBOX.

        Args:
            subject: Email subject
            body: Email body (HTML format)
            image_paths: List of paths to images to embed inline in the email
            attachment_file_path: Path to a file to attach (optional)
            recipient_emails: List of recipient email addresses
            cc_emails: List of CC email addresses
        """
        try:
            html_body = body
            attachments = []

            # Embedded images - same cid approach as the old SMTP/MIMEImage
            # version, expressed as Graph fileAttachments with isInline=True
            for image_path in image_paths or []:
                if not os.path.exists(image_path):
                    continue

                content_id = str(uuid.uuid4())
                html_body += f'<br><img src="cid:{content_id}" />'

                with open(image_path, "rb") as img_file:
                    img_bytes = img_file.read()

                content_type = mimetypes.guess_type(image_path)[0] or "application/octet-stream"

                attachments.append({
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": os.path.basename(image_path),
                    "contentType": content_type,
                    "contentBytes": base64.b64encode(img_bytes).decode("utf-8"),
                    "contentId": content_id,
                    "isInline": True,
                })

            # Regular file attachment
            if attachment_file_path and os.path.exists(attachment_file_path):
                with open(attachment_file_path, "rb") as attachment_file:
                    attachment_bytes = attachment_file.read()

                content_type = mimetypes.guess_type(attachment_file_path)[0] or "application/octet-stream"

                attachments.append({
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": os.path.basename(attachment_file_path),
                    "contentType": content_type,
                    "contentBytes": base64.b64encode(attachment_bytes).decode("utf-8"),
                })

            message = {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": f"<html><body>{html_body}</body></html>",
                },
                "toRecipients": [
                    {"emailAddress": {"address": addr}} for addr in recipient_emails
                ],
            }

            if cc_emails:
                message["ccRecipients"] = [
                    {"emailAddress": {"address": addr}} for addr in cc_emails
                ]

            if attachments:
                message["attachments"] = attachments

            payload = {"message": message, "saveToSentItems": True}

            token = get_token()
            url = f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/sendMail"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 202:
                print(f"Email sent successfully from {MAILBOX} to {recipient_emails}.")
            else:
                print(f"Error sending email: status {response.status_code} - {response.text}")

        except Exception as ex:
            print(f"Error sending email: {str(ex)}")


# Example usage:
if __name__ == "__main__":
    mailer = Mailer()

    subject = "Test Email with Attachment"
    body = "<h1>Hello World!</h1><p>This is a test email with an attachment.</p>"
    image_paths = []
    # Update this path to wherever you save sample_attachment.txt locally
    # (e.g. same folder as this script, or a full absolute path)
    attachment_file_path = "sample_attachment.txt"
    recipient_emails = ["shishank@bombayshavingcompany.com"]
    cc_emails = ["shishank@bombayshavingcompany.com"]

    mailer.send_email(subject, body, image_paths, attachment_file_path, recipient_emails, cc_emails)