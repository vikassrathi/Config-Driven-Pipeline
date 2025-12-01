import streamlit as st
import pandas as pd
from components.lineage_component import add_lineage_step

from cleaning.fill_null_data import FillNullData
from cleaning.remove_null_data import RemoveNullData
from cleaning.rename_columns import RenameColumns
from cleaning.remove_duplicate import RemoveDuplicatesCleaning
from cleaning.drop_columns import DropColumnsCleaning


def page_data_cleaning(app_instance):
    """
    Handles the UI for configuring data cleaning rules.
    """
    st.title("Data Cleaning Rules")

    # Check if any data is available
    if app_instance.current_df is None or app_instance.current_df.empty:
        st.error("No data available for cleaning. Please go back to 'Select Data Source' and upload a file.")
        if st.button("Go to Data Source Selection", key="back_to_data_source_from_cleaning_error"):
            app_instance.page = "Select Data Source"
            st.rerun()
        return

    st.write(f"Configuring cleaning rules for data.")


    st.subheader("Select Data for Cleaning")

    # Determine available dataframes
    data_source_options = ["Original Data (source)"]
    if app_instance.validated_clean_df is not None and not app_instance.validated_clean_df.empty:
        data_source_options.append("Validated Clean Data")
    if app_instance.validated_error_df is not None and not app_instance.validated_error_df.empty:
        data_source_options.append("Validated Error Data")

    selected_data_for_cleaning_key = st.radio(
        "Apply cleaning steps to:",
        options=data_source_options,
        key="selected_df_for_cleaning",
        horizontal=True
    )

    # Set the base DataFrame for cleaning
    df_to_clean_base = None
    if selected_data_for_cleaning_key == "Original Data (source)":
        df_to_clean_base = app_instance.current_df
        st.info(f"Using Original Data. Shape: {df_to_clean_base.shape}")
    elif selected_data_for_cleaning_key == "Validated Clean Data":
        df_to_clean_base = app_instance.validated_clean_df
        st.info(f"Using Validated Clean Data. Shape: {df_to_clean_base.shape}")
    elif selected_data_for_cleaning_key == "Validated Error Data":
        df_to_clean_base = app_instance.validated_error_df
        st.info(f"Using Validated Error Data. Shape: {df_to_clean_base.shape}")

    if df_to_clean_base is None or df_to_clean_base.empty:
        st.warning(
            f"The selected data source '{selected_data_for_cleaning_key}' is empty. Please select another source or ensure data is loaded.")
        return

    available_columns = df_to_clean_base.columns.tolist()

    # Display current data and validation errors
    with st.expander(f"Show Selected Data for Cleaning ({selected_data_for_cleaning_key})"):
        st.write("This is the data that will be used for current cleaning operations:")
        st.dataframe(df_to_clean_base.head())
        st.info(f"Shape: {df_to_clean_base.shape}")

    # The previous "Show Validation Error Data" expander logic:
    # if app_instance.validated_error_df is not None and not app_instance.validated_error_df.empty and selected_data_for_cleaning_key != "Validated Error Data":
    #     with st.expander("Show Validation Error Data"):
    #         st.write("These rows failed the data validation rules:")
    #         st.dataframe(app_instance.validated_error_df.head())
    #         st.info(f"Shape: {app_instance.validated_error_df.shape}")
    # elif selected_data_for_cleaning_key != "Validated Error Data":
    #     st.info("No validation errors found in the previous step, or no validation was performed.")

    st.write("---")
    st.subheader("Add Data Cleaning Step")

    cleaning_type_options = [
        "Fill Missing Values",
        "Remove Missing Values",
        "Rename Columns",
        "Drop Columns",
        "Remove Duplicates"
    ]
    selected_cleaning_type = st.selectbox(
        "Select Cleaning Operation",
        cleaning_type_options,
        key="select_cleaning_type"
    )
    new_cleaning_step_params = {}

    if selected_cleaning_type == "Fill Missing Values":
        col_fill_1, col_fill_2 = st.columns(2)
        with col_fill_1:
            col_to_fill = st.selectbox("Column to fill missing values", available_columns, key="fill_col")

        # Determine column type
        column_is_numeric = False
        if col_to_fill and not df_to_clean_base.empty and col_to_fill in df_to_clean_base.columns:
            if pd.api.types.is_numeric_dtype(df_to_clean_base[col_to_fill]):
                column_is_numeric = True

        with col_fill_2:
            if column_is_numeric:
                fill_strategy = st.radio(
                    "Filling Strategy",
                    ['fill_mean', 'fill_median', 'fill_specific'],
                    key="fill_strategy_numeric",
                    horizontal=True
                )
            else:
                fill_strategy = st.radio(
                    "Filling Strategy",
                    ['fill_mode', 'fill_specific'],
                    key="fill_strategy_non_numeric",
                    horizontal=True
                )

        fill_value = None
        if fill_strategy == 'fill_specific':
            default_fill_value = ""
            if column_is_numeric:
                default_fill_value = "0"
            else:
                default_fill_value = "UNKNOWN"

            fill_value = st.text_input("Specific value to fill with", value=default_fill_value,
                                       key="fill_specific_value")

        if st.button("Add Fill Missing Values Step"):
            try:
                if fill_value:
                    if column_is_numeric:
                        try:
                            fill_value = float(fill_value) if '.' in fill_value else int(fill_value)
                        except ValueError:
                            st.error(f"Please enter a valid numerical value for '{col_to_fill}'.")
                            return
                step = FillNullData(app_instance, app_instance.utils, add_lineage_step,
                                    column=col_to_fill, strategy=fill_strategy, fill_value=fill_value)
                app_instance.cleaning_steps.append(step)
                st.success(f"Added: {step.get_display_name()}")
            except ValueError as e:
                st.error(f"Configuration error: {e}")

    elif selected_cleaning_type == "Remove Missing Values":
        remove_col_1, remove_col_2 = st.columns(2)
        with remove_col_1:
            remove_strategy = st.radio(
                "Removal Strategy",
                ['drop_rows_any', 'drop_rows_col', 'drop_column'],
                key="remove_strategy",
                horizontal=True
            )
        columns_for_drop_rows = None
        column_for_drop_col = None

        with remove_col_2:
            if remove_strategy == 'drop_rows_col':
                columns_for_drop_rows = st.multiselect(
                    "Columns to check for nulls (drop row if null in selected)",
                    available_columns,
                    key="cols_for_drop_rows"
                )
            elif remove_strategy == 'drop_column':
                column_for_drop_col = st.selectbox(
                    "Column to drop if it contains any nulls",
                    available_columns,
                    key="col_for_drop_col"
                )

        if st.button("Add Remove Missing Values Step"):
            try:
                step = RemoveNullData(app_instance, app_instance.utils, add_lineage_step,
                                      strategy=remove_strategy, columns_to_check=columns_for_drop_rows,
                                      column_to_drop=column_for_drop_col)
                app_instance.cleaning_steps.append(step)
                st.success(f"Added: {step.get_display_name()}")
            except ValueError as e:
                st.error(f"Configuration error: {e}")

    elif selected_cleaning_type == "Rename Columns":
        rename_col_1, rename_col_2 = st.columns(2)
        with rename_col_1:
            rename_option = st.radio(
                "Rename Type",
                ['specific_rename', 'to_lowercase', 'to_uppercase', 'to_snake_case', 'to_camel_case', 'trim_spaces'],
                key="rename_option",
                horizontal=False
            )
        old_name_input = None
        new_name_input = None
        cols_to_transform = None

        with rename_col_2:
            if rename_option == 'specific_rename':
                old_name_input = st.selectbox("Old Column Name", available_columns, key="old_col_name")
                new_name_input = st.text_input("New Column Name", key="new_col_name")
            else:
                select_all_columns = st.checkbox("Apply to all columns", key="rename_select_all_cols")
                if select_all_columns:
                    cols_to_transform = available_columns

                    st.multiselect(
                        f"Select Columns to convert to {rename_option.replace('to_', '').replace('_', ' ')}",
                        available_columns,
                        default=available_columns,
                        key="cols_to_transform",
                        disabled=True
                    )
                else:
                    cols_to_transform = st.multiselect(
                        f"Select Columns to convert to {rename_option.replace('to_', '').replace('_', ' ')}",
                        available_columns,
                        key="cols_to_transform"
                    )

        if st.button("Add Rename Columns Step"):
            try:
                step = RenameColumns(app_instance, app_instance.utils, add_lineage_step,
                                     rename_type=rename_option,
                                     old_name=old_name_input, new_name=new_name_input,
                                     columns_to_transform=cols_to_transform)
                app_instance.cleaning_steps.append(step)
                st.success(f"Added: {step.get_display_name()}")
            except ValueError as e:
                st.error(f"Configuration error: {e}")

    elif selected_cleaning_type == "Drop Columns":
        columns_to_drop_permanently = st.multiselect(
            "Select columns to permanently remove",
            available_columns,
            key="cols_to_drop_permanently"
        )
        if st.button("Add Drop Columns Step"):
            if columns_to_drop_permanently:
                try:
                    step = DropColumnsCleaning(app_instance, app_instance.utils, add_lineage_step,
                                               columns_to_drop=columns_to_drop_permanently)
                    app_instance.cleaning_steps.append(step)
                    st.success(f"Added: {step.get_display_name()}")
                except ValueError as e:
                    st.error(f"Configuration error: {e}")
            else:
                st.warning("Please select at least one column to drop.")

    elif selected_cleaning_type == "Remove Duplicates":
        if st.button("Add Remove Duplicates Step"):
            try:
                step = RemoveDuplicatesCleaning(app_instance, app_instance.utils, add_lineage_step)
                app_instance.cleaning_steps.append(step)
                st.success(f"Added: {step.get_display_name()}")
            except ValueError as e:
                st.error(f"Configuration error: {e}")

    st.write("---")
    st.subheader("Configured Cleaning Steps:")
    if not app_instance.cleaning_steps:
        st.info("No cleaning steps added yet.")
    else:
        for i, step in enumerate(app_instance.cleaning_steps):
            col_display, col_remove = st.columns([0.8, 0.2])
            with col_display:
                st.write(f"**{i + 1}.** {step.get_display_name()}")
            with col_remove:
                if st.button(f"Remove {i + 1}", key=f"remove_cleaning_step_{i}"):
                    app_instance.cleaning_steps.pop(i)
                    st.success(f"Removed step {i + 1}.")
                    st.rerun()

    st.write("---")

    # --- Preview Cleaning Button ---
    if st.button("Preview Cleaning", key="preview_cleaning_button"):
        if df_to_clean_base is not None:
            st.info("Applying configured cleaning operations for preview...")
            preview_df = app_instance.utils.apply_cleaning_steps(
                df_to_clean_base.copy(),
                app_instance.cleaning_steps
            )
            app_instance.preview_cleaned_df = preview_df
            st.success("Preview generated!")
            # Manually set a state variable to show the expander
            st.session_state['show_preview_cleaned_df'] = True
        else:
            st.error("No data available to preview cleaning. Please ensure data is loaded.")

    if 'show_preview_cleaned_df' in st.session_state and st.session_state['show_preview_cleaned_df'] and hasattr(
            app_instance, 'preview_cleaned_df') and app_instance.preview_cleaned_df is not None:
        with st.expander("Preview of Cleaned Data"):
            st.write("This is the DataFrame after applying the configured cleaning operations:")
            st.dataframe(app_instance.preview_cleaned_df.head())
            st.info(f"Shape: {app_instance.preview_cleaned_df.shape}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Back: Data Validation", key="back_from_cleaning_to_validation"):
            app_instance.page = "Data Validation"
            # Clear preview state if going back
            if 'show_preview_cleaned_df' in st.session_state:
                del st.session_state['show_preview_cleaned_df']
            if hasattr(app_instance, 'preview_cleaned_df'):
                del app_instance.preview_cleaned_df
            st.rerun()
    with col2:
        if st.button("Next: Data Transformation", key="next_from_cleaning_to_transformation"):
            st.info("Applying all configured cleaning operations to the selected dataset and moving to next step...")

            if df_to_clean_base is not None:
                app_instance.cleaned_df = app_instance.utils.apply_cleaning_steps(
                    df_to_clean_base,
                    app_instance.cleaning_steps
                )


                cleaning_config_for_yaml = {
                    "cleaning_steps": [step.get_config_for_yaml() for step in app_instance.cleaning_steps]
                }
                app_instance.utils.create_json_config(
                    app_instance,
                    component_type="cleaning",
                    component_config_data=cleaning_config_for_yaml,
                    output_filename=f"{app_instance.pipeline_name}_cleaning_config.json",
                    output_directory="cleaning"
                )

                st.success(f"Data cleaned and ready for transformation. Shape: {app_instance.cleaned_df.shape}")


                if 'show_preview_cleaned_df' in st.session_state:
                    del st.session_state['show_preview_cleaned_df']
                if hasattr(app_instance, 'preview_cleaned_df'):
                    del app_instance.preview_cleaned_df

                app_instance.page = "Data Transformation"
                st.rerun()
            else:
                st.error("No data to apply cleaning to. Please load data first.")
