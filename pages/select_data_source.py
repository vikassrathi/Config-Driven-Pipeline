import streamlit as st
import pandas as pd
import utils
import time


from data_sources.onelake_data_source import OnelakeDataSource
from data_sources.local_file_data_source import LocalFileDataSource
from data_sources.adls_data_source import AdlsDataSource

# Import lineage function from components
from components.lineage_component import add_lineage_step


def page_select_data_source(app_instance):
    """
    Handles the UI for selecting the data source (Onelake, Local File, or ADLS).
    It orchestrates interactions with the concrete data source implementations.
    """
    st.title("Select Data Source")

    # Check for Fabric connection details before proceeding
    # if not app_instance.fabric_workspace_id or not app_instance.fabric_lakehouse_id or not app_instance.jwt_token:
    #     st.error("Fabric connection details are missing.")
    #     if st.button("Go to Login for Fabric Setup", key="back_to_fabric_setup_from_data_source"):
    #         app_instance.page = "Login"
    #         st.rerun()
    #     return

    data_source_option = st.selectbox(
        "Choose a Data Source",
        ["Local File", "Onelake", "ADLS"],
        key="data_source_selection_type"
    )

    # Initialize the correct data source handler based on selection
    if "data_source_handler" not in st.session_state or \
            app_instance.data_source_option_prev != data_source_option:

        # If switching data source type, reset the old source's state first
        if "data_source_handler" in st.session_state and st.session_state["data_source_handler"] is not None:
            st.session_state["data_source_handler"].reset_source_state()
            # Also reset the new flag for local file upload status
            if hasattr(app_instance, 'is_file_uploaded_to_lakehouse'):  # Ensure attribute exists before resetting
                app_instance.is_file_uploaded_to_lakehouse = False

        if data_source_option == "Onelake":
            st.session_state["data_source_handler"] = OnelakeDataSource(app_instance, utils, add_lineage_step)
        elif data_source_option == "Local File":
            st.session_state["data_source_handler"] = LocalFileDataSource(app_instance, utils, add_lineage_step)
        elif data_source_option == "ADLS":
            st.session_state["data_source_handler"] = AdlsDataSource(app_instance, utils, add_lineage_step)

        app_instance.data_source_option_prev = data_source_option

    current_data_source_handler = st.session_state["data_source_handler"]

    st.markdown("---")
    st.subheader(f"Configure {data_source_option} Source & execution method")
    execution_method = st.radio(
        "How should these validation rules be applied?",
        ["Fabric Notebook", "DataFlow"],
        index=0,
        key="execution_method_radio",
        help="Validation will be executed on Fabric using selected option."
    )
    app_instance.execution_method = execution_method


    if data_source_option == "Onelake":
        with st.spinner(f"Fetching tables from Onelake Lakehouse '{app_instance.fabric_lakehouse_id}'..."):
            table_options = current_data_source_handler.get_source_options()

        if table_options:
            selected_table_name = st.selectbox(
                "Select a table from Onelake",
                [""] + table_options,  # Add an empty option
                key="onelake_table_select"
            )
            if "table_selection_done" not in st.session_state:
                st.session_state.table_selection_done = False
            # st.write(selected_table_name)
            if not st.session_state.table_selection_done:
                if selected_table_name:
                    current_data_source_handler.set_active_selection(selected_table_name)
                    app_instance.add_lineage_step(  # Use the injected lineage function
                        app_instance,
                        "Data Source Selected",
                        details={"source_type": "Onelake", "table_name": selected_table_name}
                    )
                    st.info(
                     f"Selected Onelake table: '{selected_table_name}'")
                    st.session_state.table_selection_done = True
                else:
                    current_data_source_handler.set_active_selection(None)
            # st.session_state.table_selection_done = True
        else:
            st.warning(
                f"No tables found in Lakehouse '{app_instance.fabric_lakehouse_id}' or failed to fetch tables. Please check your Fabric connection details and Lakehouse content.")
            current_data_source_handler.set_active_selection(None)  # Ensure selection is cleared


    elif data_source_option == "Local File":
        #TODO move this logic whole into active selection method in local_file_data_soruce.py file
        uploaded_file = st.file_uploader("Upload CSV or Excel File", type=["csv", "xlsx"], key="local_file_uploader")

        # Inputs for Lakehouse table name and format options
        target_lakehouse_table_name_input = st.text_input(
            "Desired Lakehouse Table Name",
            value=app_instance.target_lakehouse_table_name,
            help="Name for the new table that will be created in your Lakehouse from this file.",
            key="new_lakehouse_table_name_input"
        )
        app_instance.target_lakehouse_table_name = target_lakehouse_table_name_input

        col_format_1, col_format_2 = st.columns(2)
        with col_format_1:
            file_format_input = st.selectbox("File Format", ["CSV", "PARQUET"], key="uploaded_file_format")
        with col_format_2:
            has_header_input = st.checkbox("File has Header", value=True, key="uploaded_file_has_header")

        delimiter_input = ","
        if file_format_input == "CSV":
            delimiter_input = st.text_input("Delimiter (for CSV)", value=",", key="uploaded_file_delimiter")

        format_options_input = {
            "format": file_format_input,
            "header": has_header_input,
            "delimiter": delimiter_input
        }
        app_instance.uploaded_file_format_options = format_options_input

        if uploaded_file:
            # Check if set_active_selection was successful
            if current_data_source_handler.set_active_selection(
                    uploaded_file,
                    app_instance.target_lakehouse_table_name,
                    app_instance.uploaded_file_format_options
            ):
                st.success(f"File '{uploaded_file.name}' loaded successfully locally!")
                st.info(f"Shape: {app_instance.current_df.shape}")
                st.write("First 5 rows of your data:")
                st.dataframe(app_instance.current_df.head())
                st.info(f"Local file prepared. Click 'Upload to Lakehouse & Create Table' below to proceed.")
            else:
                # If the same file was re-selected and not re-processed, still show current_df
                if app_instance.current_df is not None and not app_instance.current_df.empty:
                    st.info(f"File '{uploaded_file.name}' is already loaded locally.")
                    st.info(f"Shape: {app_instance.current_df.shape}")
                    st.write("First 5 rows of your data:")
                    st.dataframe(app_instance.current_df.head())
                    st.info(f"Local file prepared. Click 'Upload to Lakehouse & Create Table' below to proceed.")
                else:
                    st.warning("Please upload a file to proceed with local file processing.")

        else:
            # If no file is currently in the uploader, clear relevant app_instance state
            # This logic branch will also reset is_file_uploaded_to_lakehouse to False via reset_source_state
            if app_instance.selected_table_is_local and app_instance.uploaded_file_object is not None:
                current_data_source_handler.reset_source_state()
                st.rerun()
            elif app_instance.current_df is not None:  # If a DF is present but no file uploaded, show it
                st.info(f"Currently loaded: '{app_instance.selected_table}'.")
                st.write("First 5 rows of your data:")
                st.dataframe(app_instance.current_df.head())

        # Dedicated Upload Button
        if st.button("Upload to Lakehouse & Create Table", key="upload_to_lakehouse_button"):
            # Use app_instance attributes directly for robustness
            if app_instance.uploaded_file_object and app_instance.target_lakehouse_table_name:

                current_file_key = (app_instance.uploaded_file_object.name, app_instance.uploaded_file_object.size)
                should_proceed_upload = True

                # Check if this file was already successfully uploaded to Fabric
                if app_instance.last_uploaded_fabric_file_key and current_file_key == app_instance.last_uploaded_fabric_file_key:
                    st.info("This file appears to have been already uploaded to Lakehouse and converted to a table.")
                    force_reupload = st.checkbox("Force Re-upload and Overwrite Table?", key="force_reupload_same_file")
                    should_proceed_upload = force_reupload
                    if not should_proceed_upload:
                        st.warning(
                            "Re-upload cancelled. You can proceed to Data Validation if the previous upload was successful.")
                        # This 'return' prevents the API calls if not forcing re-upload
                        return

                if should_proceed_upload:
                    # Use the new upload function from utils
                    with st.spinner(
                            f"Uploading '{app_instance.uploaded_file_object.name}' to Lakehouse Files and preparing for table conversion..."):

                        # fetch lakehouse_name if not already set
                        # if not app_instance.fabric_lakehouse_name:
                        #     st.warning("Lakehouse display name not found in app_instance. Attempting to retrieve...")
                        #     try:
                        #         # Get workspaces and then find the lakehouse by ID to get its display name
                        #         workspaces = utils.list_fabric_workspaces(app_instance)
                        #         for ws in workspaces:
                        #             if ws['id'] == app_instance.fabric_workspace_id:
                        #                 lakehouses_endpoint = f"/v1/workspaces/{ws['id']}/items?$filter=type eq 'Lakehouse'"
                        #                 fabric_items_response = utils.call_fabric_api(app_instance, "GET",
                        #                                                               lakehouses_endpoint)
                        #                 lakehouses = [item for item in fabric_items_response.get('value', []) if
                        #                               item['type'] == 'Lakehouse']
                        #                 for lh in lakehouses:
                        #                     if lh['id'] == app_instance.fabric_lakehouse_id:
                        #                         app_instance.fabric_lakehouse_name = lh['displayName']
                        #                         st.info(
                        #                             f"Retrieved Lakehouse display name: {app_instance.fabric_lakehouse_name}")
                        #                         break
                        #             if app_instance.fabric_lakehouse_name:  # Stop if found
                        #                 break
                        #     except Exception as e:
                        #         st.error(
                        #             f"Could not retrieve Lakehouse display name: {e}. Please ensure it's set during login.")
                        #         # Fallback if name cannot be retrieved, will cause error in upload_file_to_lakehouse_files
                        #         app_instance.fabric_lakehouse_name = "default_lakehouse_name"  # Placeholder

                        upload_success, upload_message = utils.upload_file_to_lakehouse_files(
                            app_instance,
                            app_instance.uploaded_file_object,
                        )

                        if upload_success:
                            st.success(upload_message)
                            relative_file_path_in_lakehouse = upload_message

                            file_format = app_instance.uploaded_file_format_options.get("format", "CSV")
                            has_header = app_instance.uploaded_file_format_options.get("header", True)
                            delimiter = app_instance.uploaded_file_format_options.get("delimiter", ",")

                            st.info(
                                f"Initiating conversion of '{relative_file_path_in_lakehouse}' to Lakehouse table '{app_instance.target_lakehouse_table_name}'...")

                            try:
                                operation_url = utils.initiate_load_to_lakehouse_table(
                                    app_instance,
                                    relative_file_path_in_lakehouse,
                                    app_instance.target_lakehouse_table_name,
                                    file_format,
                                    has_header,
                                    delimiter
                                )

                                if operation_url:
                                    # st.success("Table load operation initiated. Waiting for completion...")
                                    # app_instance.is_file_uploaded_to_lakehouse is already False on initial file selection


                                    with st.status("Polling Fabric to check Table loaded",
                                                   expanded=True) as status_box:
                                        # poll_count = 0
                                        # max_polls = 4  # Max 5 minutes (60 * 5 seconds)
                                        status = {"status": ""}  # Initial status

                                        while status["status"] == "Running":
                                            time.sleep(5)
                                            status = utils.poll_notebook_status(app_instance, operation_url)
                                            # st.write(status)


                                        if status["status"] == "Completed":
                                            status_box.update(label="File successfully loaded into Lakehouse table!",
                                                              state="complete", expanded=False)
                                            app_instance.add_lineage_step(
                                                app_instance,
                                                "Loaded to Lakehouse Table",
                                                details={"table_name": app_instance.target_lakehouse_table_name,
                                                         "source_file": app_instance.uploaded_file_object.name,
                                                         "status": "Completed"}
                                            )
                                            app_instance.is_file_uploaded_to_lakehouse = True  # Set success flag
                                            # Update last_uploaded_fabric_file_key on successful load to table
                                            app_instance.last_uploaded_fabric_file_key = current_file_key
                                            st.success(
                                                f"File '{app_instance.uploaded_file_object.name}' successfully loaded into Lakehouse table '{app_instance.target_lakehouse_table_name}'. You can now proceed to Data Validation.")

                                            # current_df head after successful Fabric upload confirmation
                                            # if app_instance.current_df is not None:
                                            #     st.write("First 5 rows of your data (re-confirming after Fabric load):")
                                            #     st.dataframe(app_instance.current_df.head())

                                        elif status["status"] == "Failed":
                                            st.info("File uploaded but failed to load into Lakehouse table.")
                                            # error_msg = status.get('error',
                                            #                        'Unknown error. Check Fabric logs for details.')
                                            # status_box.update(
                                            #     label=f"Failed to load file to Lakehouse table: {error_msg}",
                                            #     state="error", expanded=True)
                                            # app_instance.add_lineage_step(
                                            #     app_instance,
                                            #     "Load to Lakehouse Table Failed",
                                            #     details={"table_name": app_instance.target_lakehouse_table_name,
                                            #              "source_file": app_instance.uploaded_file_object.name,
                                            #              "status": "Failed", "error": error_msg}
                                            # )
                                            app_instance.is_file_uploaded_to_lakehouse = False
                                        else:
                                            status_box.update(
                                                label=f"Table load operation status: {status['status']}.",
                                                state="warning", expanded=True)
                                            app_instance.is_file_uploaded_to_lakehouse = False  # Not completed or failed in time
                                            st.warning(
                                                "Table load operation did not complete ")

                            except Exception as e:
                                st.error(f"An unexpected error occurred during Load to Tables operation: {e}")
                                app_instance.add_lineage_step(
                                    app_instance,
                                    "Load to Lakehouse Table Failed",
                                    details={"table_name": app_instance.target_lakehouse_table_name,
                                             "source_file": app_instance.uploaded_file_object.name, "status": "Error",
                                             "error": str(e)}
                                )
                                app_instance.is_file_uploaded_to_lakehouse = False

                        else:
                            st.error(f"File upload to Lakehouse Files failed: {upload_message}")
                            app_instance.add_lineage_step(
                                app_instance,
                                "File Upload to Lakehouse Files Failed",
                                details={"file_name": app_instance.uploaded_file_object.name, "error": upload_message}
                            )
                            app_instance.is_file_uploaded_to_lakehouse = False
            else:
                st.warning("Please upload a file and provide a desired Lakehouse table name first.")


    elif data_source_option == "ADLS":
        st.info("ADLS integration is currently conceptual. Please select a predefined ADLS path below.")
        adls_paths = current_data_source_handler.get_source_options()
        selected_adls_path = st.selectbox(
            "Select an ADLS Path",
            [""] + adls_paths,
            key="adls_path_select"
        )
        if selected_adls_path:
            current_data_source_handler.set_active_selection(selected_adls_path)
            st.success(f"ADLS Path selected: '{selected_adls_path}'")
            st.info("No data will be loaded into the application for ADLS in this phase.")
        else:
            current_data_source_handler.set_active_selection(None)


    st.markdown("---")
    if st.button("Next: Go to Data Validation", key="next_from_data_source_to_validation"):
        if current_data_source_handler.is_selection_valid():
            app_instance.page = "Data Validation"
            st.rerun()
        else:
            st.warning("Please make a valid data source selection before proceeding.")
            if data_source_option == "Onelake":
                st.info("Please select a table from Onelake.")
            elif data_source_option == "Local File":
                st.info(
                    "Please upload a local CSV or Excel file, provide a desired Lakehouse table name, and ensure the file is successfully uploaded."
                )
            elif data_source_option == "ADLS":
                st.info("Please select an ADLS path.")

