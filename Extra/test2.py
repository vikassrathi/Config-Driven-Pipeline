import streamlit as st
import pandas as pd
import pyodbc
from msal import PublicClientApplication
import jwt

# Fabric config - Load from environment variables
import os
TENANT_ID = os.getenv('FABRIC_TENANT_ID', '')
CLIENT_ID = os.getenv('FABRIC_CLIENT_ID', '')
SQL_ENDPOINT = os.getenv('FABRIC_SQL_ENDPOINT', '')
DATABASE = os.getenv('FABRIC_DATABASE', 'selfserving')

# Reuse your working function for device auth
def get_fabric_jwt_token_device_flow(tenant_id: str, client_id: str):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope_fabric = ["https://api.fabric.microsoft.com/.default"]
    app = PublicClientApplication(client_id=client_id, authority=authority)

    result = app.acquire_token_interactive(scopes=scope_fabric)
    if result and "access_token" in result:
        decoded = jwt.decode(result["access_token"], options={"verify_signature": False})
        st.success(f"Logged in as: {decoded.get('upn') or decoded.get('preferred_username')}")
        return result["access_token"]
    else:
        st.error("Authentication failed.")
        return None

# Connect using token from device flow
def connect_sql_with_token(access_token):
    conn_str = f"""
    Driver={{ODBC Driver 18 for SQL Server}};
    Server=tcp:{SQL_ENDPOINT},1433;
    Database={DATABASE};
    Authentication=ActiveDirectoryAccessToken;
    Encrypt=yes;
    TrustServerCertificate=no;
    """
    token_bytes = bytes(access_token, "utf-8")
    token_struct = b"\x01" + token_bytes + b"\x00"
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    return conn

# Streamlit UI
st.title("Fabric Lakehouse Table Explorer")

if "access_token" not in st.session_state:
    token = get_fabric_jwt_token_device_flow(TENANT_ID, CLIENT_ID)
    if token:
        st.session_state["access_token"] = token

if "access_token" in st.session_state:
    conn = connect_sql_with_token(st.session_state["access_token"])
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.tables")
        tables = [row[0] for row in cursor.fetchall()]
        selected_table = st.selectbox("Select a table", tables)

        if st.button("Preview Top 5 Rows"):
            df = pd.read_sql(f"SELECT TOP 5 * FROM {selected_table}", conn)
            st.session_state["current_df"] = df
            st.subheader(f"Preview of `{selected_table}`")
            st.dataframe(df)

            with st.expander("📦 Download CSV"):
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button("Download CSV", csv, f"{selected_table}_top5.csv", "text/csv")



# If silent fails, initiate device code flow
        # flow = app.initiate_device_flow(scopes=scope_fabric)
        # if "user_code" not in flow:
        #     st.error("Failed to initiate device flow. Please check tenant ID and client ID.")
        #     return None
        #
        # # Display instructions to the user
        # st.warning(f"Please open this URL in your browser: {flow['verification_uri']}")
        # st.warning(f"Then enter the following code: **{flow['user_code']}**")
        # st.warning("Waiting for user authentication in browser... This Streamlit page will automatically re-run once authentication is complete.")

        # Poll for token until user authenticates or timeout
        # Note: Streamlit's execution model means the script re-runs on user interaction.
        # This loop is designed to be called repeatedly by Streamlit's reruns until a token is obtained.
        # We store the flow in session state to continue polling across reruns.
        # if "fabric_auth_flow" not in st.session_state:
        #     st.session_state.fabric_auth_flow = flow

        # This will block and poll, but Streamlit reruns make it appear interactive
        # result = app.acquire_token_by_device_flow(st.session_state.fabric_auth_flow)


        # if result and "access_token" in result:
        #     st.success("Acquired Fabric token successfully via device flow.")
        #     del st.session_state.fabric_auth_flow # Clear flow once token is obtained
        #     return result['access_token']
        # elif "error" in result and result["error"] == "authorization_pending":
        #     # Still waiting for user to complete authentication in browser
        #     return None # Don't return error, just continue waiting
        # else:
        #     if "error_description" in result:
        #         st.error(f"Error acquiring Fabric token: {result['error_description']}")
        #     else:
        #         st.error(f"Failed to acquire Fabric token: {result}")
        #     del st.session_state.fabric_auth_flow # Clear flow on definitive error
        #     return None


# class AccessTokenCredential:
#     """
#     A custom credential class for Azure SDKs that wraps an existing access token.
#     This is used to authenticate DataLakeServiceClient with the JWT token obtained
#     from Fabric authentication.
#     """
#
#     def __init__(self, access_token: str):
#         self._token = access_token
#
#     def get_token(self, *scopes, **kwargs) -> AccessToken:
#         # The 'expires_on' part is crucial for Azure SDKs to consider the token valid.
#         # For simplicity, we'll use a placeholder future timestamp (1 hour from now)
#         # as the actual expiry might not be easily available from just the JWT string.
#         return AccessToken(self._token, int(time.time()) + 3600)  # Token valid for 1 hour
#

# @st.cache_resource
# def get_datalake_service_client(_app_instance) -> DataLakeServiceClient:
#     """
#     Provides a cached DataLakeServiceClient authenticated with the current JWT token.
#     This avoids re-authenticating or initiating device flow for file operations.
#     """
#     # Use _app_instance for caching purposes as recommended by Streamlit