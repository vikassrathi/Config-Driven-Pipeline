import os
import streamlit as st
import json
import base64
import time
import graphviz
import pandas as pd


import utils
from config import config


# Import page functions from the 'pages' package
from pages import (
    login,
    select_data_source,
    data_validation,
    data_cleaning,
    data_transformation,
    load_data,
    pipeline_success,
    pipeline_final
)

from fabric_auth_helper import get_fabric_jwt_token_device_flow
from components.lineage_component import render_lineage_graph, add_lineage_step


# Load users from config
USERS = config.MOCK_USERS


class PipelineApp:
    """
    Manages the entire Streamlit application's state in an OOP manner.
    This class is instantiated once and stored in st.session_state.
    Page functions and components interact with this single instance.
    """

    def __init__(self):
        """Initializes application state, reading from st.session_state if available."""
        # Initialize attributes that will be synced with st.session_state
        self.page = st.session_state.get("page", "Login")
        self.dev_mode_skip_login = config.DEV_MODE_SKIP_LOGIN
        # This will store the real Fabric token after auth
        self.jwt_token = st.session_state.get("jwt_token", None)
        # This will store the mock token for internal app login (separate from Fabric token)
        self.app_login = st.session_state.get("jwt_token_app_login", None)
        self.is_app_logged_in = st.session_state.get("is_app_logged_in", False) # New flag for app login state
        self.onelake_token = st.session_state.get("onelake_token", None)
        self.pipeline_name = st.session_state.get("pipeline_name", "")
        self.data_destination = st.session_state.get("data_destination", "Onelake")
        self.output_table = st.session_state.get("output_table", "")

        # DataFrames and processing steps
        self.current_df = st.session_state.get("current_df", None)
        self.uploaded_file_object = st.session_state.get("uploaded_file_object", None)
        self.uploaded_df = st.session_state.get("uploaded_df", None)
        self.selected_table = st.session_state.get("selected_table", None)
        self.selected_table_is_local = st.session_state.get("selected_table_is_local", False)
        self.table_data = st.session_state.get("table_data", {})

        # Validation specific state
        self.validations = st.session_state.get("validations", [])
        self.validated_clean_df = st.session_state.get("validated_clean_df", None)
        self.validated_error_df = st.session_state.get("validated_error_df", None)
        self.execution_method = st.session_state.get("execution_method", "Fabric Notebook")

        # Cleaning specific state
        self.cleaning_steps = st.session_state.get("cleaning_steps", [])
        self.cleaned_df = st.session_state.get("cleaned_df", None)
        self.preview_cleaned_df = st.session_state.get("preview_cleaned_df", None)

        # Transformation specific state (placeholder for now)
        self.transformations = st.session_state.get("transformations", [])

        # Lineage tracking
        self.lineage_history = st.session_state.get("lineage_history", [])

        # Data source previous selection for handler re-initialization
        self.data_source_option_prev = st.session_state.get("data_source_option_prev", None)

        # Fabric Connection Details (loaded from config or session state)
        self.fabric_tenant_id = st.session_state.get("fabric_tenant_id", config.FABRIC_TENANT_ID)
        self.fabric_client_id = st.session_state.get("fabric_client_id", config.FABRIC_CLIENT_ID)
        self.fabric_client_secret = st.session_state.get("fabric_client_secret", config.FABRIC_CLIENT_SECRET)
        self.fabric_workspace_id = st.session_state.get("fabric_workspace_id", config.FABRIC_WORKSPACE_ID)
        self.fabric_workspace_name = st.session_state.get("fabric_workspace_name", config.FABRIC_WORKSPACE_NAME)
        self.fabric_lakehouse_id = st.session_state.get("fabric_lakehouse_id", config.FABRIC_LAKEHOUSE_ID)
        self.fabric_lakehouse_name = st.session_state.get("fabric_lakehouse_name", config.FABRIC_LAKEHOUSE_NAME)

        # For local file upload to Lakehouse table
        self.target_lakehouse_table_name = st.session_state.get("target_lakehouse_table_name", "")
        self.uploaded_file_fabric_relative_path = st.session_state.get("uploaded_file_fabric_relative_path", "")
        self.uploaded_file_format_options = st.session_state.get("uploaded_file_format_options", {})
        # Track the key of the last successfully uploaded file to Fabric
        self.last_uploaded_fabric_file_key = st.session_state.get("last_uploaded_fabric_file_key", None)

        # Reference to Streamlit and utils for convenience in cleaning/validation classes
        self.st = st
        self.utils = utils
        self.add_lineage_step = add_lineage_step


    def sync_state_to_session(self):
        """Syncs all relevant attributes to st.session_state for persistence."""
        for attr, value in self.__dict__.items():
            if not attr.startswith('st') and not attr.startswith('utils') and not attr.startswith('add_lineage_step'):
                 st.session_state[attr] = value

def main_app():
    """Main function to run the Streamlit application."""

    if "app_instance" not in st.session_state:
        st.session_state.app_instance = PipelineApp()
        # st.session_state.fabric_token,st.session_state.onelake_token=get_fabric_jwt_token_device_flow('ca98711b-e9cf-409e-91ff-c06b2e168a51','ecc69692-31cf-4ba9-82c8-0bc3746ca78c')
    app_instance = st.session_state.app_instance

    render_lineage_graph(app_instance)
    # st.write(app_instance)
    # if app_instance.dev_mode_skip_login:
    #     app_instance.fabric_tenant_id = ''
    #     app_instance.fabric_client_id = ''
    #     app_instance.fabric_client_secret = ''
    #     app_instance.fabric_workspace_id = ''
    #     app_instance.fabric_workspace_name = ''
    #     app_instance.fabric_lakehouse_id = ''
    #     app_instance.fabric_lakehouse_name = ''
    #     app_instance.jwt_token=st.session_state.fabric_token
    #     app_instance.onelake_token=st.session_state.onelake_token
    #     app_instance.page='Select Data Source'
    #     select_data_source.page_select_data_source(app_instance)

        # Simplified page flow: Always go to login first to handle both app and Fabric auth
    if app_instance.page == "Login":
        login.page_login(app_instance, USERS)
    # Once the app is logged in AND Fabric details (token, workspace, lakehouse) are set

    elif (app_instance.is_app_logged_in and app_instance.jwt_token and app_instance.fabric_workspace_id and app_instance.fabric_lakehouse_id):
        if app_instance.page == "Select Data Source":
            select_data_source.page_select_data_source(app_instance)
        elif app_instance.page == "Data Validation":
            data_validation.page_data_validation(app_instance)
        elif app_instance.page == "Data Cleaning":
            data_cleaning.page_data_cleaning(app_instance)
        elif app_instance.page == "Data Transformation":
            data_transformation.page_data_transformation(app_instance)
        elif app_instance.page == "Load Data":
            load_data.page_load_data(app_instance)
        elif app_instance.page == "Pipeline Success":
            pipeline_success.page_pipeline_success(app_instance)
        elif app_instance.page == "Pipeline Final":
            pipeline_final.page_pipeline_final(app_instance)
        else: # Fallback for unexpected pages after full login/fabric setup
            st.error("Invalid page state. Redirecting to Select Data Source.")
            app_instance.page = "Select Data Source"
            st.rerun()
    else: # If app is not logged in, or Fabric details are missing after app login, redirect to login
        st.warning("Please complete app login and Fabric connection setup.")
        app_instance.page = "Login"
        st.rerun()


    app_instance.sync_state_to_session()


if __name__ == "__main__":
    main_app()
