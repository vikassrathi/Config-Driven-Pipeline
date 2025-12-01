import streamlit as st
import json
from fabric_auth_helper import get_fabric_jwt_token_device_flow
from utils import call_fabric_api
import time
# --- Constants ---
TENANT_ID = 'ca98711b-e9cf-409e-91ff-c06b2e168a51'
CLIENT_ID = 'ecc69692-31cf-4ba9-82c8-0bc3746ca78c'
WORKSPACE_ID = "5c794792-12f8-4e81-86a5-784ececf5b16"
# NOTEBOOK_ID = "7d30a52b-4cd6-450c-b788-a734b226362b"
NOTEBOOK_ID='90183c3f-06c5-4d0f-bc46-8d2c274393ed'

# --- AppInstance class ---
class AppInstance:
    def __init__(self, workspace_id, jwt_token):
        self.fabric_workspace_id = workspace_id
        self.jwt_token = jwt_token

# --- Function to call/run a Fabric notebook ---
def call_fabric_notebook(app_instance, notebook_id: str, notebook_params: dict = None):
    endpoint = f"/v1/workspaces/{app_instance.fabric_workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    payload = {}

    if notebook_params:
        # payload["executionData"] = json.dumps(notebook_params)
        payload["executionData"] =notebook_params

    st.info(f"Starting notebook `{notebook_id}`...")

    try:
        response = call_fabric_api(app_instance, "POST", endpoint, payload=payload)
        if response.get("status") == "success" and "Location" in response:
            st.success("Notebook execution started!")
            st.code(response["Location"], language="bash")
            return response["Location"]
        else:
            st.error("Notebook execution did not start successfully.")
            st.json(response)
    except Exception as e:
        st.error(f"Exception: {e}")
    return None

# --- Function to poll notebook status and display output ---
def poll_notebook_status(app_instance, location_url):
    st.info("Checking notebook execution status...")

    while True:
        try:
            endpoint = location_url.replace("https://api.fabric.microsoft.com", "")
            time.sleep(10)
            response = call_fabric_api(app_instance, "GET", endpoint)
            status = response.get("status")
            # st.write(status)
            st.write(f"Notebook status: **{status}**")

            if status in ["Completed", "Failed", "Cancelled"]:
                st.subheader("Raw notebook job response:")
                st.json(response)

                # Try to display output logs if available
                output_logs = response.get("output", {}).get("value", None)
                if output_logs:
                    st.subheader("Notebook Output")
                    st.code(output_logs)
                #
                # # Also show error message if present
                # error_details = response.get("error", {}).get("message")
                # if error_details:
                #     st.subheader("Error Details")
                #     st.error(error_details)

                break

            time.sleep(10)
        except Exception as e:
            st.error(f"Error checking notebook status: {e}")
            break

# --- Streamlit App UI ---
st.title("Fabric Notebook Runner")

# Step 1: Authenticate
if st.button("Authenticate"):
    try:
        token_str, _ = get_fabric_jwt_token_device_flow(TENANT_ID, CLIENT_ID)
        st.session_state["jwt_token"] = token_str
        st.success("JWT token acquired")
    except Exception as e:
        st.error(f"Authentication failed: {e}")

# Step 2: Run notebook if authenticated
if "jwt_token" in st.session_state:
    if st.button("Run Notebook"):
        notebook_params={"pipeline_name": "", "execution_method": "Fabric Notebook", "validation_config": {"great_expectations_suite": {"data_asset_type": "PandasDataset", "expectations": [{"expectation_type": "expect_column_to_exist", "kwargs": {"column": "video_view5"}}, {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "video_view5"}}]}}}
        app_instance = AppInstance(WORKSPACE_ID, st.session_state["jwt_token"])
        location_url = call_fabric_notebook(app_instance, NOTEBOOK_ID,notebook_params)
        if location_url:
            poll_notebook_status(app_instance, location_url)