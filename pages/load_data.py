import streamlit as st
from utils import json_to_base64, create_pipeline_api
from components.lineage_component import add_lineage_step


def page_load_data(app_instance):
    """
    Handles loading data and creating the pipeline.
    Interacts with the app_instance to update state.
    """
    st.title("Load Data")

    if app_instance.current_df is None or app_instance.current_df.empty:
        st.error(
            "Error: No processed data available to load.")
        if st.button("Go back to Data Source Selection", key="back_from_load_no_df"):
            app_instance.page = "Select Data Source"
            st.rerun()
        return

    st.write(f"Loading data from: **{app_instance.selected_table}**")
    st.write(f"DataFrame Shape for Load: {app_instance.current_df.shape}")
    st.dataframe(app_instance.current_df.head())

    pipeline_name_input = st.text_input(
        "Enter Pipeline Name",
        value=app_instance.pipeline_name,
        key="pipeline_name_input"
    )
    app_instance.pipeline_name = pipeline_name_input

    data_destination = st.selectbox("Choose Data Destination", ["Onelake"], key="destination_select")
    app_instance.data_destination = data_destination

    output_table_input = st.text_input(
        "Output Table Name",
        value=app_instance.output_table,
        key="output_table_input"
    )
    app_instance.output_table = output_table_input

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Back: Data Transformation", key="back_to_transformation"):
            app_instance.page = "Data Transformation"
            st.rerun()
    with col2:
        if st.button("Confirm & Create Pipeline", key="confirm_create_pipeline_button"):
            if not app_instance.pipeline_name:
                st.error("Please enter a Pipeline Name.")
                return
            if not app_instance.output_table:
                st.error("Please enter an Output Table Name.")
                return
            if app_instance.current_df is None or app_instance.current_df.empty:
                st.error("No data to load. Please ensure data is loaded and cleaned/transformed.")
                return

            filter_transformations = [
                t for t in app_instance.transformations if t.get("type") == "Filter"
            ]
            remove_column_transformations = [
                t for t in app_instance.transformations if t.get("type") == "Remove Column"
            ]

            parameters = {}
            if remove_column_transformations:
                parameters["remove_column"] = {
                    "value": remove_column_transformations[0]["columns"][0] if remove_column_transformations[0][
                        "columns"] else "",
                    "type": "string"
                }
            if filter_transformations:
                parameters["filter_column_value"] = {
                    "value": f"{filter_transformations[0]['column']}:{filter_transformations[0]['value']}",
                    "type": "string"
                }
            st.rerun()
