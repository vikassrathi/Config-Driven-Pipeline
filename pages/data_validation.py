import streamlit as st
import pandas as pd
from components.lineage_component import add_lineage_step
import os
import json
import time

from utils import read_file_from_onelake
from validation.not_null_validation import NotNullValidation
from validation.datatype_validation import DataTypeValidation
from validation.mandatory_columns_validation import MandatoryColumnsValidation


def page_data_validation(app_instance):
    """
    Handles the UI for configuring Great Expectations data validation rules.
    Triggers Fabric Notebook validation and saves GE configuration.
    """
    st.title("Data Validation Rules")

    # Determine the data source and display relevant
    if app_instance.selected_table_is_local:
        st.write(f"Configuring validation rules **{app_instance.selected_table}**")
        if app_instance.current_df is None or app_instance.current_df.empty:
            st.error(
                "No local data available for validation. Please go back to 'Select Data Source' and upload a file.")
            if st.button("Go to Data Source Selection", key="back_to_data_source_from_validation_error_local"):
                app_instance.page = "Select Data Source"
                st.rerun()
            return
        st.write(f"Current DataFrame Shape: {app_instance.current_df.shape}")
        st.dataframe(app_instance.current_df.head())
        available_columns = app_instance.current_df.columns.tolist()
    else:
        st.write(f"Configuring validation rules for table: **{app_instance.selected_table}**")
        if app_instance.current_df is None or app_instance.current_df.empty or app_instance.selected_table_schema is None:
            st.error(
                "No schema or sample data available for Onelake table. Please re-select the table on 'Select Data Source' page.")
            if st.button("Go to Data Source Selection", key="back_to_data_source_from_validation_error_onelake"):
                app_instance.page = "Select Data Source"
                st.rerun()
            return
        st.info("Displaying top 5 rows from table:")
        st.dataframe(app_instance.current_df.head())


        available_columns = [col['name'] for col in app_instance.selected_table_schema]
        st.write(f"Schema for the table: {', '.join(available_columns)}")


    st.subheader("Fabric Pipeline Execution Method")
    if not hasattr(app_instance, 'execution_method'):
        app_instance.execution_method = "Fabric Notebook"



    st.markdown("---")
    st.subheader("Define Validation Rules")

    # Initialize validations list if not present
    # if "validations" not in st.session_state:
    #     st.session_state.validations = []

    # Display existing rules
    if app_instance.validations:
        st.markdown("Current Rules:")
        for i, val_obj in enumerate(app_instance.validations):
            col1, col2 = st.columns([0.8, 0.2])
            with col1:
                st.write(f"{i + 1}. {val_obj.get_display_name()}")
            with col2:
                if st.button(f"Remove {i + 1}", key=f"remove_validation_{i}"):
                    app_instance.validations.pop(i)
                    st.rerun()
        st.markdown("---")


    st.markdown("Add New Rule:")
    validation_type = st.selectbox(
        "Select Validation Type",
        ["", "Mandatory Columns", "Not Null", "Data Type"],
        key="validation_type_select"
    )

    if validation_type == "Mandatory Columns":
        column_to_check = st.selectbox(
            "Select Column",
            [""] + available_columns,
            key="mandatory_col_select"
        )
        if st.button("Add Mandatory Column Rule", key="add_mandatory_col_rule"):
            if column_to_check:
                new_validation = MandatoryColumnsValidation(app_instance, app_instance.utils,
                                                            app_instance.add_lineage_step, column_to_check)
                if new_validation.is_valid_for_pipeline():
                    app_instance.validations.append(new_validation)
                    new_validation.add_lineage(details={"column": column_to_check})
                    st.success(f"Added mandatory column rule for '{column_to_check}'.")
                    st.rerun()
                else:
                    st.warning("Please select a valid column.")
            else:
                st.warning("Please select a column.")

    elif validation_type == "Not Null":
        column_to_check = st.selectbox(
            "Select Column",
            [""] + available_columns,
            key="not_null_col_select"
        )
        if st.button("Add Not Null Rule", key="add_not_null_rule"):
            if column_to_check:
                new_validation = NotNullValidation(app_instance, app_instance.utils, app_instance.add_lineage_step,
                                                   column_to_check)
                if new_validation.is_valid_for_pipeline():
                    app_instance.validations.append(new_validation)
                    new_validation.add_lineage(details={"column": column_to_check})
                    st.success(f"Added not null rule for '{column_to_check}'.")
                    st.rerun()
                else:
                    st.warning("Please select a valid column.")
            else:
                st.warning("Please select a column.")

    elif validation_type == "Data Type":
        col1, col2 = st.columns(2)
        with col1:
            column_to_check = st.selectbox(
                "Select Column",
                [""] + available_columns,
                key="datatype_col_select"
            )
        with col2:
            expected_type = st.selectbox(
                "Expected Data Type",
                ["", "string", "int", "float", "boolean", "datetime"],
                key="expected_type_select"
            )
        if st.button("Add Data Type Rule", key="add_datatype_rule"):
            if column_to_check and expected_type:
                new_validation = DataTypeValidation(app_instance, app_instance.utils, app_instance.add_lineage_step,
                                                    column_to_check, expected_type)
                if new_validation.is_valid_for_pipeline():
                    app_instance.validations.append(new_validation)
                    new_validation.add_lineage(details={"column": column_to_check, "type": expected_type})
                    st.success(f"Added data type rule for '{column_to_check}' as '{expected_type}'.")
                    st.rerun()
                else:
                    st.warning("Please select a valid column and expected type.")
            else:
                st.warning("Please select a column and an expected type.")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Back: Select Data Source", key="back_from_validation_to_source"):
            app_instance.page = "Select Data Source"
            st.rerun()
    with col2:
        if st.button("Perform Validation", key="perform_validation_button"):
            if not app_instance.validations:
                st.warning("Please add at least one validation rule.")
                return

            # --- Build and Save the GE Expectation Suite to JSON ---
            ge_expectation_suite_for_json = {
                "data_asset_type": "PandasDataset",  # For Fabric, this would be "SparkDFDataset"
                "expectations": [val_obj.get_ge_expectation_config() for val_obj in app_instance.validations if
                                 val_obj.is_valid_for_pipeline()]
            }

            if not ge_expectation_suite_for_json["expectations"]:
                st.error("No valid validation rules to apply. Please check your rule configurations.")
                return

            app_instance.utils.create_json_config(
                app_instance,
                component_type="validation",
                component_config_data={"great_expectations_suite": ge_expectation_suite_for_json},
                output_filename=f"{app_instance.pipeline_name}_ge_validation_config.json",
                output_directory="validation"
            )

            fabric_validation_notebook_id = "90183c3f-06c5-4d0f-bc46-8d2c274393ed"

            table_to_validate = ""
            if app_instance.selected_table_is_local:
                table_to_validate = app_instance.target_lakehouse_table_name
            else:
                table_to_validate = app_instance.selected_table

            if not table_to_validate:
                st.error("Cannot determine table to validate. Please ensure a data source is properly selected.")
                return

            # Parameters for the Fabric Validation Notebook
            # st.write(ge_expectation_suite_for_json)
            notebook_params = {
                "table_name": f"{app_instance.fabric_lakehouse_name}.{table_to_validate}",
                "ge_expectation_suite_json": ge_expectation_suite_for_json  # Pass suite as JSON string
            }

            st.info(f"Triggering Fabric Notebook for validation of table '{table_to_validate}'...")

            operation_url = None
            try:
                with st.spinner("Initiating Fabric Validation Notebook..."):

                    operation_url = app_instance.utils.call_fabric_notebook(app_instance, fabric_validation_notebook_id,
                                                                            notebook_params)
            except Exception as e:
                st.error(f"Failed to initiate Fabric Validation Notebook: {e}")
                return
            file_path='selfserving.Lakehouse/Files/temp_data'
            clean_file_name='cleandf.csv'
            error_file_name='errordf.csv'
            if operation_url:
                with st.status("Polling Fabric Validation Notebook Status", expanded=False) as status_box:
                    status = {"status":''}
                    # status_box.write(f"Notebook status: {status['status']}")
                    time.sleep(5)
                    response = app_instance.utils.poll_notebook_status(app_instance, operation_url)
                    status['status'] = response.get("status")

                    if status["status"] == "Completed":
                            st.info('Select cleandf file')
                            app_instance.validated_clean_df = read_file_from_onelake(app_instance, file_path,
                                                                                     clean_file_name)
                            st.info('Select errordf file')
                            app_instance.validated_error_df = read_file_from_onelake(app_instance, file_path,
                                                                                     error_file_name)

                            st.success(
                            f"Validation rules applied to '{table_to_validate}' on Fabric. Check Fabric Notebook logs for results.")
                            add_lineage_step(app_instance, "Validation Notebook Triggered (Fabric)",
                                         details={"table": table_to_validate, "status": "Succeeded"})


                    elif status["status"] == "Failed":
                            error_msg = status.get('error', 'Unknown error. Check Fabric logs for details.')
                            status_box.update(label=f"Fabric Validation Notebook failed: {error_msg}", state="error",
                                          expanded=True)
                            st.error("Fabric Validation Notebook failed. Please check its logs in Fabric for details.")
                            add_lineage_step(app_instance, "Validation Notebook Triggered",
                                             details={"table": table_to_validate, "status": "Failed", "error": error_msg})
                    else:
                        status_box.update(
                            label=f"Notebook status: {status['status']}. Please manually check in Fabric for final status.",
                            state="warning", expanded=True)
                        st.warning(
                            "Fabric Validation Notebook did not complete in time. Please check Fabric portal for actual status.")
                        add_lineage_step(app_instance, "Validation Notebook Triggered",
                                         details={"table": table_to_validate, "status": "Timeout/Pending"})
                # TODO check this logic it can fail if status of notebook comes failed so how to fix it
                if hasattr(app_instance, "validated_clean_df") and hasattr(app_instance, "validated_error_df"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("Cleaned DataFrame")
                        st.dataframe(app_instance.validated_clean_df,use_container_width=True)
                        st.info(len(app_instance.validated_clean_df))
                    with col2:
                        st.subheader("Error DataFrame")
                        st.dataframe(app_instance.validated_error_df,use_container_width=True)
                        st.info(len(app_instance.validated_error_df))
            else:
                st.error("Failed to initiate Fabric Validation Notebook. No operation URL received.")
    if st.button("Next: Go to Data Cleaning", key="next_from_data_validation_to_cleaning"):
        app_instance.page = "Data Cleaning"
        st.rerun()

