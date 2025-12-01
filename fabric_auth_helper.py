from azure.identity import DeviceCodeCredential
from msal import PublicClientApplication
import requests
import jwt
import streamlit as st
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.credentials import AccessToken
import time

from config import config


def get_fabric_jwt_token_device_flow(tenant_id: str, client_id: str):
    """
    Acquires a JWT token for Microsoft Fabric using the Device Code Flow.
    This is suitable for applications that don't have a secure backend to store secrets
    or perform redirects (like command-line tools or Streamlit apps that can't host a redirect URI).

    Args:
        tenant_id (str): Your Azure AD Tenant ID.
        client_id (str): Your Azure AD Application (client) ID.

    Returns:
        tuple: (fabric_token, onelake_service_client) if successful, (None, None) otherwise.
    """
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope_fabric = config.FABRIC_SCOPE
    scope_onelake = config.ONELAKE_SCOPE
    account_url = config.ONELAKE_DFS_BASE_URL

    app = PublicClientApplication(
        client_id=client_id,
        authority=authority
    )

    try:
        result = app.acquire_token_silent(scopes=scope_fabric, account=None)
        if result and "access_token" in result:
            st.success("Acquired Fabric token silently from cache.")
        else:
            # result = app.acquire_token_interactive(scopes=scope_fabric)
            result = app.acquire_token_interactive(scopes=scope_fabric)
            # prompt = "consent"
            if result and "access_token" in result:
                # st.write(result["access_token"])
                decoded=jwt.decode(result["access_token"], options={"verify_signature": False})
                st.write("Logged in as:", decoded.get("upn") or decoded.get("preferred_username"))
                st.success("Acquired Fabric token via interactive login.")
            else:
                st.error("Failed to acquire Fabric token.")
                return None, None

            credential = DeviceCodeCredential()
            one_lake_token=DataLakeServiceClient(account_url=account_url, credential=credential)
            return (result['access_token'],one_lake_token)

    except Exception as e:
        st.error(f"An unexpected error occurred during Fabric token or onelake acquisition: {e}")
        return None




