import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe
df = pd.DataFrame({"passenger_count": [1, 2, 3, 4]})
validation_objects={"ExpectColumnValuesToNotBeNull":"actions.video_view"}
def perform_ge_validation_and_split_local(df: pd.DataFrame, validation_objects: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
# Create an ephemeral Data Context
    context = gx.get_context(mode="ephemeral")

# Define names
    data_source_name = "my_data_source"
    data_asset_name = "my_dataframe_data_asset"
    batch_definition_name = "my_batch_definition"

# Register pandas data source
    data_source = context.data_sources.add_pandas(name=data_source_name)

# Add a DataFrame asset
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# Define a batch definition
    batch_def = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# Sample DataFrame (7 will fail the expectation)


    # Get batch
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # Create expectation suite
    suite = gx.ExpectationSuite(name="passenger_suite")


    for expectation_name, column_name in validation_objects.items():
        expectation_class = getattr(gxe, expectation_name)
        suite.add_expectation(
        expectation_class(column=column_name)
    )

    # Run validation
    result = batch.validate(suite)
    print(result)


perform_ge_validation_and_split_local(df,validation_objects)




def upload_file_to_lakehouse_files(app_instance, uploaded_file_object, destination_folder: str = "user_uploads") -> \
tuple[bool, str]:
    """
    Uploads a file to the Lakehouse's staging area and then moves it to a specified folder
    within the 'Files' section.
    Args:
        app_instance: The PipelineApp instance containing JWT token, workspace ID, lakehouse ID.
        uploaded_file_object: The Streamlit UploadedFile object.
        destination_folder (str): The subfolder within 'Files/' to move the file to.
                                  e.g., 'user_uploads', 'raw_data'.
    Returns:
        tuple[bool, str]: (True, relative_path_in_lakehouse) on success, (False, error_message) on failure.
    """
    if not app_instance.fabric_workspace_id or \
            not app_instance.fabric_lakehouse_id or \
            not app_instance.jwt_token:
        return False, "Fabric connection details are missing. Cannot upload file."

    file_name = uploaded_file_object.name
    file_content = uploaded_file_object.getvalue()  # Get binary content of the uploaded file

    # --- Step 1: Upload to Staging Area ---
    staging_endpoint = f"/v1/workspaces/{app_instance.fabric_workspace_id}/lakehouses/{app_instance.fabric_lakehouse_id}/staging/{file_name}"

    st.info(f"Attempting to upload '{file_name}' to Lakehouse staging...")
    try:
        # Use call_fabric_api for PUT request, passing binary data
        upload_response = call_fabric_api(app_instance, "PUT", staging_endpoint, data_binary=file_content)
        # Check if response status is success (200, 201, 204 typically)
        if upload_response.get("status") != "success":  # call_fabric_api now returns dict with status
            return False, f"Failed to upload to staging: {upload_response.get('message', 'Unknown error')}"
        st.success(f"Successfully uploaded '{file_name}' to staging.")
    except requests.exceptions.RequestException as e:
        return False, f"Staging upload failed: {e}"
    except Exception as e:
        return False, f"An unexpected error occurred during staging upload: {e}"

    # --- Step 2: Move from Staging to Final Destination in 'Files' ---
    final_destination_path = f"Files/{destination_folder}/{file_name}"
    move_endpoint = f"/v1/workspaces/{app_instance.fabric_workspace_id}/lakehouses/{app_instance.fabric_lakehouse_id}/paths/move"
    move_body = {
        "sourcePath": f"/staging/{file_name}",
        "destinationPath": final_destination_path,
        "action": "move"
    }

    st.info(f"Attempting to move '{file_name}' from staging to '{final_destination_path}'...")
    try:
        # Use call_fabric_api for POST request to move
        move_response = call_fabric_api(app_instance, "POST", move_endpoint, payload=move_body)
        if move_response.get("status") != "success":
            return False, f"Failed to move file: {move_response.get('message', 'Unknown error')}"
        st.success(f"Successfully moved '{file_name}' to '{final_destination_path}'.")
        return True, final_destination_path
    except requests.exceptions.RequestException as e:
        return False, f"File move operation failed: {e}"
    except Exception as e:
        return False, f"An unexpected error occurred during file move: {e}"