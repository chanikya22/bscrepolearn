"""
Graph auth for excel_sync -- application (app-only) permissions via the client
credentials flow. No sign-in, no device code, no human in the loop, ever.

Requires the app registration to have been granted these as APPLICATION
permissions (not delegated), with admin consent:
    - Sites.Selected
    - Files.ReadWrite.All  (or Files.Read.All if the app only ever reads)

Sites.Selected still requires a one-time per-site grant -- that's a separate
step from this file and doesn't change based on app-only vs delegated: an
admin runs a single POST to /sites/{site-id}/permissions, once per SharePoint
site, naming this application as the grantee. See README. It is not a login
and does not need repeating per sheet or per run.
"""
import os
import msal

GRAPH_CLIENT_ID = "67cb3478-5032-41ed-a259-da3f6cf35490"
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
GRAPH_TENANT_ID = "fc748222-60a9-4e23-bb8e-6c1b97dc6d8f"  # must be a real tenant id/domain for app-only; "common" does not work here
AUTHORITY = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]  # app permissions are requested via .default, not individual scope names

# Built once per process and reused: MSAL caches the app token in memory and
# refreshes it near expiry, so repeated calls within the same task are cheap.
_app = msal.ConfidentialClientApplication(
    GRAPH_CLIENT_ID,
    client_credential=GRAPH_CLIENT_SECRET,
    authority=AUTHORITY,
)


def get_graph_token() -> str:
    """Returns a valid Graph access token. No interaction, no cache file, ever."""
    result = _app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Could not acquire Graph token: {result.get('error_description')}")
    return result["access_token"]
