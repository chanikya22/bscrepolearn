"""
Read the latest matching email's body from a mailbox using Microsoft Graph
(app-only / client-credentials flow) instead of IMAP.

Same signature and return contract as the old IMAP version:
    get_latest_email_body(sender_email, email_subject) -> body str | None | str(exception)

Env vars required (.env): GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET,
MAIL_READ_MAILBOX

NOTE: reading mail needs a Mail.Read (Application) permission, admin-consented,
separate from Mail.Send - and if you have an Application Access Policy scoping
mailboxes for this app (see the sendMail troubleshooting), MAIL_READ_MAILBOX
needs to be included in that scope too, or Graph will 403 here the same way.

pip install msal requests python-dotenv
"""

import os
from datetime import datetime, timezone

import msal
import requests
from dotenv import load_dotenv
import environmentconfig

load_dotenv()

TENANT_ID = os.getenv("GRAPH_TENANT_ID")
CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
MAILBOX = os.getenv("MAIL_READ_MAILBOX")

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


def get_latest_email_body(sender_email, email_subject, debug=True):
    try:
        token = get_token()

        since = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")

        url = f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/mailFolders/inbox/messages"
        headers = {
            "Authorization": f"Bearer {token}",
            "Prefer": 'outlook.body-content-type="text"',  # ask for plain text body
        }
        params = {
            # Only date filtered server-side - from/emailAddress/address eq
            # is known to be unreliable for some senders in Graph, so sender
            # + subject matching is done client-side below instead.
            "$filter": f"receivedDateTime ge {since}",
            "$orderby": "receivedDateTime desc",
            "$top": 50,
            "$select": "subject,from,receivedDateTime,body",
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            if debug:
                print(f"[DEBUG] Graph request failed: {response.status_code} - {response.text}")
            return None

        messages = response.json().get("value", [])

        if debug:
            print(f"[DEBUG] Fetched {len(messages)} message(s) received today from {MAILBOX}'s inbox")
            for m in messages[:10]:
                sender = m.get("from", {}).get("emailAddress", {}).get("address", "unknown")
                print(f"[DEBUG]   {m.get('receivedDateTime')} | {sender} | {m.get('subject')}")

        sender_needle = sender_email.lower()
        subject_needle = email_subject.lower()

        for msg in messages:
            msg_sender = (msg.get("from", {}).get("emailAddress", {}).get("address") or "").lower()
            msg_subject = (msg.get("subject") or "").lower()

            if sender_needle in msg_sender and subject_needle in msg_subject:
                return msg.get("body", {}).get("content")

        if debug:
            print("[DEBUG] No message matched both sender and subject in the fetched batch.")
        return None

    except Exception as ex:
        return str(ex)


if __name__ == "__main__":
    result = get_latest_email_body("Anmol.Dhupar@dfmfoods.com", "Your OTP for PartnersBiz login")
    print(result)