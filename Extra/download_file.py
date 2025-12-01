import streamlit as st
from msal import PublicClientApplication
import jwt
from azure.identity import DeviceCodeCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import io
# Configuration - Load from environment variables
import os
TENANT_ID = os.getenv('FABRIC_TENANT_ID', '')
CLIENT_ID = os.getenv('FABRIC_CLIENT_ID', '')
FILESYSTEM = os.getenv('FABRIC_WORKSPACE_NAME', 'selfservingpipeline')
LAKEHOUSE_FOLDER = f"{os.getenv('FABRIC_LAKEHOUSE_NAME', 'selfserving')}.Lakehouse/Files/temp_data"
ACCOUNT_URL = "https://onelake.dfs.fabric.microsoft.com"


# 1. Authenticate user and store token/client
def authenticate():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    scope_fabric = ["https://api.fabric.microsoft.com/.default"]

    app = PublicClientApplication(client_id=CLIENT_ID, authority=authority)
    result = app.acquire_token_silent(scopes=scope_fabric, account=None)

    if not result:
        result = app.acquire_token_interactive(scopes=scope_fabric)

    if result and "access_token" in result:
        decoded = jwt.decode(result["access_token"], options={"verify_signature": False})
        st.session_state["token"] = result["access_token"]
        st.session_state["username"] = decoded.get("upn") or decoded.get("preferred_username")

        credential = DeviceCodeCredential()
        client = DataLakeServiceClient(account_url=ACCOUNT_URL, credential=credential)
        st.session_state["client"] = client
        st.success(f"Logged in as: {st.session_state['username']}")
    else:
        st.error("Failed to acquire Fabric token")


# UI
st.title("Download File from Microsoft OneLake")

if "client" not in st.session_state:
    if st.button("Authenticate"):
        authenticate()
else:
    st.success(f"Authenticated as: {st.session_state['username']}")
    client = st.session_state["client"]

    try:
        file_system = client.get_file_system_client(FILESYSTEM)
        directory_client = file_system.get_directory_client(LAKEHOUSE_FOLDER)
        paths = list(directory_client.get_paths())
        st.write(paths)
        file_names = [p.name.split("/")[-1] for p in paths if not p.is_directory and p.name.split(".")[-1]!='crc']

        if not file_names:
            st.warning("No files found in folder.")
        else:
            selected_file = st.selectbox("Select file to download", file_names)

            import os

            if selected_file:
                file_client = directory_client.get_file_client(selected_file)
                data = file_client.download_file().readall()

                ext = os.path.splitext(selected_file)[-1].lower()

                try:
                    if ext == ".csv":
                        df = pd.read_csv(io.BytesIO(data))
                    elif ext == ".json":
                        df = pd.read_json(io.BytesIO(data))
                    else:
                        st.warning(f"Unsupported file type: {ext}")
                        df = None

                    if df is not None:
                        preview_df = df.head(5)
                        st.session_state["current_df"] = preview_df
                        st.subheader("Preview (Top 5 Rows)")
                        st.dataframe(preview_df, use_container_width=True)

                    st.download_button("Download File", data=data, file_name=selected_file)
                    file_client.delete_file()
                    st.success(f"File '{selected_file}' read and deleted from OneLake.")
                except Exception as e:
                    st.error(f"Failed to load or preview file: {e}")

    except Exception as e:
        st.error(f"Failed to list/download: {e}")
