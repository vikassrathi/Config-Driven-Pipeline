import streamlit as st
import time # Import time for st.spinner
from utils import test_fabric_connection # Import the new test function

def page_fabric_connection_setup(app_instance):
    """
    Handles the UI for setting up Fabric connection details (Workspace, Lakehouse IDs, and JWT Token).
    """
    st.title("Fabric Connection Setup")
    st.markdown("---")
    st.write("Please provide your Microsoft Fabric Workspace and Lakehouse details, along with an authentication token.")
    st.info("Note: The connection will be tested via Fabric API calls. Ensure your JWT Token has appropriate permissions.")

    # Input for Fabric Workspace ID
    fabric_workspace_id_input = st.text_input(
        "Fabric Workspace ID",
        value=app_instance.fabric_workspace_id,
        help="The unique ID of your Microsoft Fabric workspace. (e.g., '12345678-abcd-1234-abcd-1234567890ab')",
        key="fabric_workspace_id_input"
    )

    # Input for Fabric Lakehouse ID
    fabric_lakehouse_id_input = st.text_input(
        "Fabric Lakehouse ID",
        value=app_instance.fabric_lakehouse_id,
        help="The unique ID of the Lakehouse within your workspace where data will be stored. (e.g., 'abcdef12-3456-7890-abcd-1234567890ef')",
        key="fabric_lakehouse_id_input"
    )

    # Input for JWT Token (Authentication Token)
    jwt_token_input = st.text_input(
        "JWT Token (Azure AD Access Token)",
        value=app_instance.jwt_token if app_instance.jwt_token else "", # Pre-fill if already available
        help="An Azure AD JWT (Access Token) to authenticate with Fabric APIs. This is a sensitive credential and should be handled securely.",
        type="password", # Mask the input for security
        key="jwt_token_setup_input"
    )


    col1, col2 = st.columns(2)
    with col1:
        if st.button("Back to Login", key="back_to_login_from_fabric_setup"):
            app_instance.page = "Login"
            st.rerun()

    with col2:
        if st.button("Test Connection & Next", key="test_connection_button"):
            if not fabric_workspace_id_input or not fabric_lakehouse_id_input or not jwt_token_input:
                st.error("Please enter Fabric Workspace ID, Lakehouse ID, and JWT Token.")
                return

            # Store the IDs and token in the app instance immediately for use by test_fabric_connection
            app_instance.fabric_workspace_id = fabric_workspace_id_input
            app_instance.fabric_lakehouse_id = fabric_lakehouse_id_input
            app_instance.jwt_token = jwt_token_input

            with st.spinner("Testing connection to Fabric..."):
                success, message = test_fabric_connection(app_instance) # Call the live API test function

            if success:
                st.success(message)
                app_instance.page = "Select Data Source"
                st.rerun()
            else:
                st.error(f"Connection failed: {message}. Please check your inputs and permissions.")
