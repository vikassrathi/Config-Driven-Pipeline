import streamlit as st

from fabric_auth_helper import get_fabric_jwt_token_device_flow, DEFAULT_TENANT_ID, DEFAULT_CLIENT_ID
from utils import list_fabric_workspaces


def page_login(app_instance, users):
    """
    Handles the user login via username and password, and then Fabric authentication setup.
    """
    st.title("Login & Fabric Setup")
    st.markdown("---")

    # --- Streamlit Mock Login ---
    st.subheader("1. Streamlit Application Login")
    st.write("Enter credentials to access the Streamlit app interface.")
    username_input = st.text_input("Username", key="username_input_mock")
    password_input = st.text_input("Password", type="password", key="password_input")

    # Initialize app_instance.is_app_logged_in if it doesn't exist
    if not hasattr(app_instance, 'is_app_logged_in'):
        app_instance.is_app_logged_in = False

    if st.button("Login to App", key="login_app_button"):
        if username_input in users and users[username_input]["password"] == password_input:
            app_instance.app_login = users[username_input]["token"]
            app_instance.is_app_logged_in = True
            st.success("App Login successful! Proceed to Fabric setup.")
            st.rerun()  # Rerun to show Fabric auth section
        else:
            app_instance.is_app_logged_in = False
            st.error("Invalid username or password for app. Please try again.")

    if app_instance.is_app_logged_in and (
            not app_instance.jwt_token or app_instance.jwt_token == app_instance.app_login):
    #     st.markdown("---")
    #     st.subheader("2. Microsoft Fabric Connection Setup")
    #
    #     st.write("Provide Azure AD details to obtain an access token for Fabric APIs.")
    #     st.info("For a real app, these credentials would be securely managed. This uses Device Code Flow.")
    #
    #     tenant_id = st.text_input("Azure AD Tenant ID",
    #                               value=app_instance.fabric_tenant_id if app_instance.fabric_tenant_id else DEFAULT_TENANT_ID,
    #                               key="tenant_id_input")
    #     client_id = st.text_input("Azure AD Client ID",
    #                               value=app_instance.fabric_client_id if app_instance.fabric_client_id else DEFAULT_CLIENT_ID,
    #                               key="client_id_input")
        # Client Secret is not needed for Device Code Flow with PublicClientApplication
        # but keep a placeholder if user is used to seeing it.
        # st.text_input("Azure AD Client Secret (Not used for Device Flow)", value="", type="password", disabled=True)

        # app_instance.fabric_tenant_id = app_instance.fabric_tenant_id
        # app_instance.fabric_client_id = client_id
        # app_instance.fabric_client_secret = client_secret
        st.info('Login into fabric')
        if st.button("Get Fabric Token", key="get_fabric_token_button"):
            if not app_instance.fabric_tenant_id or not app_instance.fabric_client_id:
                st.error("Please provide Tenant ID and Client ID.")
                return

            with st.spinner("Acquiring Fabric & Onelake Access Token"):
                fabric_token,onelake_token = get_fabric_jwt_token_device_flow(app_instance.fabric_tenant_id, app_instance.fabric_client_id)

            if fabric_token and onelake_token:
                app_instance.jwt_token = fabric_token
                app_instance.onelake_token=onelake_token
                st.success("Successfully acquired Fabric Access Token! Please select Workspace/Lakehouse.")
                st.rerun()
            else:
                st.error("Failed to acquire Fabric Token. Check console for details or try again.")

    # --- Select Fabric Workspace and Lakehouse ---
    # This section appears only if a Fabric token
    if app_instance.jwt_token and app_instance.jwt_token != app_instance.app_login:
        st.markdown("---")
        st.subheader("3. Select Fabric Workspace and Lakehouse")

        if not app_instance.fabric_workspace_id:  # Only list if no workspace is  selected
            with st.spinner("Listing Workspaces..."):
                workspaces = list_fabric_workspaces(app_instance)

            workspace_names = [""] + [ws['displayName'] for ws in workspaces] if workspaces else [""]
            selected_workspace_name = st.selectbox("Select Fabric Workspace", workspace_names, key="select_workspace")

            selected_workspace_id = None
            if selected_workspace_name:
                st.write(selected_workspace_name)
                app_instance.workspace_name = selected_workspace_name
                selected_workspace_id = next(
                    (ws['id'] for ws in workspaces if ws['displayName'] == selected_workspace_name), None)

            if selected_workspace_id:
                app_instance.fabric_workspace_id = selected_workspace_id
                st.info(f"Selected Workspace ID: `{app_instance.workspace_name,app_instance.fabric_workspace_id}`")
                st.rerun()  # Rerun to populate lakehouses
            else:
                app_instance.fabric_workspace_id = ""  # Clear if no workspace selected
                app_instance.fabric_workspace_name = ""
                app_instance.fabric_lakehouse_id = ""  # Clear lakehouse too

        # Fetch and select Lakehouse within the chosen workspace
        if app_instance.fabric_workspace_id and not app_instance.fabric_lakehouse_id:
            with st.spinner(f"Listing Lakehouses in '{app_instance.fabric_workspace_id}'..."):
                try:
                    lakehouses_endpoint = f"/v1/workspaces/{app_instance.fabric_workspace_id}/items?$filter=type eq 'Lakehouse'"
                    fabric_items_response = app_instance.utils.call_fabric_api(app_instance, "GET", lakehouses_endpoint)
                    lakehouses = [item for item in fabric_items_response.get('value', []) if
                                  item['type'] == 'Lakehouse']

                    lakehouse_names = [""] + [lh['displayName'] for lh in lakehouses] if lakehouses else [""]
                    selected_lakehouse_name = st.selectbox("Select Lakehouse", lakehouse_names, key="select_lakehouse")

                    selected_lakehouse_id = None
                    if selected_lakehouse_name:
                        app_instance.lakehouse_name = selected_lakehouse_name
                        selected_lakehouse_id = next(
                            (lh['id'] for lh in lakehouses if lh['displayName'] == selected_lakehouse_name), None)

                    if selected_lakehouse_id:
                        app_instance.fabric_lakehouse_id = selected_lakehouse_id
                        st.info(f"Selected Lakehouse ID: `{app_instance.fabric_lakehouse_id}`")
                        st.rerun()  # Rerun to mark setup complete
                    else:
                        app_instance.fabric_lakehouse_id = ""
                except Exception as e:
                    st.error(f"Failed to list Lakehouses: {e}")
                    app_instance.fabric_lakehouse_id = ""

        #Input for Lakehouse Schema Name
        # if app_instance.fabric_workspace_id and app_instance.fabric_lakehouse_id:
        #     schema_name = st.text_input(
        #         "Lakehouse Schema Name (e.g., 'Tables', 'public')",
        #         value=app_instance.fabric_schema_name,
        #         help="The schema within the Lakehouse where tables reside (e.g., 'Tables' for user-created tables).",
        #         key="fabric_schema_name_input"
        #     )
        #     app_instance.fabric_schema_name = schema_name


        # If all connection details are selected
        if app_instance.fabric_workspace_id and app_instance.fabric_lakehouse_id:
            st.success(
                f"Fabric connection established with Workspace: `{app_instance.fabric_workspace_id}`, Lakehouse: `{app_instance.fabric_lakehouse_id}`")
            if st.button("Proceed to Data Source Selection", key="proceed_to_data_source"):
                app_instance.page = "Select Data Source"
                st.rerun()
            else:
                app_instance.page = "Login"
        else:
            st.warning("Please select a Fabric Workspace and Lakehouse to proceed.")
            app_instance.page = "Login"
