import streamlit as st
import json
import base64
import requests
import pandas as pd
import yaml
import os
import io
import time
import random
import great_expectations as gx
import great_expectations.expectations as gxe




def json_to_base64(json_def):
    json_string = json.dumps(json_def)
    base64_encoded_string = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')
    return base64_encoded_string


def create_pipeline_api(payload):
    st.success(f"Mocking pipeline creation for: {payload['displayName']}")

    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

        def iter_content(self, chunk_size=1024):
            yield b''

    return MockResponse(201, "Mock pipeline created successfully.")


def read_uploaded_file(uploaded_file):
    if uploaded_file is not None:
        file_details = {"filename": uploaded_file.name, "filetype": uploaded_file.type, "filesize": uploaded_file.size}
        try:
            if uploaded_file.type == "text/csv":
                df = pd.read_csv(uploaded_file)
            elif uploaded_file.type in ["application/vnd.ms-excel",
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
                df = pd.read_excel(uploaded_file)
            else:
                st.error("Unsupported file type. Please upload a CSV or Excel file.")
                return None
            return df
        except Exception as e:
            st.error(f"Error reading file: {e}")
            return None
    return None


def create_json_config(app_instance, component_type: str, component_config_data: dict, output_filename: str,
                       output_directory: str):
    """
    Creates a YAML configuration file for the given component type.
    This function creates local configuration files for documentation/versioning,
    not direct Fabric runtime input.
    """
    config = {
        "pipeline_name": app_instance.pipeline_name,
        "execution_method": app_instance.execution_method,
        f"{component_type}_config": component_config_data
    }


    os.makedirs(output_directory, exist_ok=True)

    file_path = os.path.join(output_directory, output_filename)
    with open(file_path, 'w') as file:
        json.dump(config, file, sort_keys=False)
    st.success(f"Configuration for {component_type} saved to {file_path}")


# def perform_ge_validation_and_split_local(df: pd.DataFrame, validation_objects: list):
#     """
#     Performs Great Expectations validation locally on a Pandas DataFrame and splits it
#     into clean and error DataFrames based on the first expectation result.
#     """
#     if not validation_objects:
#         st.info("No validation rules configured. Returning original DataFrame as clean.")
#         return df.copy(), pd.DataFrame()
#
#
#     context = gx.get_context(mode="ephemeral")
#
#
#     datasource_name = "my_dataframe_datasource"
#     data_connector_name = "my_data_connector"
#     data_asset_name = "my_data_asset"
#

#     datasource = context.sources.add_pandas(datasource_name)
#

#     data_asset = datasource.add_dataframe_asset(
#         name=data_asset_name,
#         dataframe=df,
#     )
#
#
#     batch_definition_name = "my_batch_definition"
#     batch_def = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
#
#
#     batch = batch_def.get_batch(batch_parameters={"dataframe": df})
#     # st.write(validation_objects)
#
#
#     suite = gx.ExpectationSuite(name="passenger_suite")
#     expectation_args_map = {
#         'ExpectColumnValuesToNotBeNull': ['column'],
#         'expect_column_values_to_be_of_type': ['column', 'type'],
#         'expect_column_to_exist': ['column'],
#     }
#
#     for obj in validation_objects:
#         expectation_name = obj.display_name_str
#         if expectation_name in expectation_args_map:
#             expectation_class = getattr(gxe, expectation_name)
#             args = {}
#             for arg in expectation_args_map[expectation_name]:
#                 if arg == 'column':
#                     args[arg] = obj.column_to_check
#                 elif arg == 'type':
#                     args[arg] = obj.expected_type
#             suite.add_expectation(expectation_class(**args))
#
#     # Run validation
#     result = batch.validate(suite)
#     unexpected_indices = set(result["results"][0]["result"]["partial_unexpected_index_list"])
#     df = df.reset_index(drop=True)
#     error_df = df.loc[list(unexpected_indices)]
#     clean_df = df.drop(index=unexpected_indices).reset_index(drop=True)
#     # st.write(clean_df,error_df)
#     return clean_df, error_df


def apply_cleaning_steps(df: pd.DataFrame, cleaning_steps: list):
    """
    Applies a list of cleaning step objects to a DataFrame sequentially.
    Each cleaning step modifies the DataFrame and adds a lineage entry.
    """
    processed_df = df.copy()
    if not cleaning_steps:
        st.info("No cleaning steps configured. Returning original DataFrame.")
        return processed_df

    st.subheader("Applying Cleaning Steps (Preview)")
    for i, step in enumerate(cleaning_steps):
        st.markdown(f"**Step {i + 1}:** {step.get_display_name()}")
        initial_shape = processed_df.shape
        processed_df = step.apply_cleaning(processed_df)
        final_shape = processed_df.shape

        details = {
            "initial_shape": str(initial_shape),
            "final_shape": str(final_shape),
            **step.config
        }
        step.add_lineage(details=details)
        st.write(f"  Shape after step: {processed_df.shape}")
        st.dataframe(processed_df.head())
        st.markdown("---")

    return processed_df





def call_fabric_api(app_instance, method: str, endpoint: str, payload: dict = None, data_binary=None) -> dict:
    """
    Central utility for making authenticated API calls to Microsoft Fabric.
    Args:
        app_instance: The PipelineApp instance containing jwt_token, workspace_id, etc.
        method (str): HTTP method (e.g., 'GET', 'POST', 'PUT').
        endpoint (str): The relative API endpoint (e.g., '/v1/workspaces/{workspaceId}/lakehouses').
        payload (dict, optional): JSON payload for POST/PUT requests. Defaults to None.
        data_binary (bytes, optional): Binary data for PUT requests (e.g., file content). Defaults to None.
    Returns:
        dict: JSON response from the Fabric API.
    Raises:
        requests.exceptions.RequestException: For network or HTTP errors.
    """
    #TODO Convert this into async function so it run in background
    base_url = "https://api.fabric.microsoft.com"
    full_url = f"{base_url}{endpoint}"

    headers = {
        "Authorization": f"Bearer {app_instance.jwt_token}",
    }
    if payload:
        headers["Content-Type"] = "application/json"

    try:
        if method.upper() == 'GET':
            response = requests.get(full_url, headers=headers)
        elif method.upper() == 'POST':
            response = requests.post(full_url, headers=headers, json=payload)
        elif method.upper() == 'PUT':
            response = requests.put(full_url, headers=headers, data=data_binary)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()

        if response.content:
            return response.json()
        else:
            result = {"status": "success", "message": "Operation completed with no content"}
            if 'Location' in response.headers:
                result['Location'] = response.headers['Location']
            return result

    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP Error: {e.response.status_code} - {e.response.text}"
        st.error(f"Fabric API call failed: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.ConnectionError as e:
        error_message = f"Connection Error: Could not connect to Fabric API. {e}"
        st.error(f"Fabric API call failed: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.Timeout as e:
        error_message = f"Timeout Error: Fabric API call timed out. {e}"
        st.error(f"Fabric API call failed: {e}")
        raise requests.exceptions.RequestException(error_message) from e
    except requests.exceptions.RequestException as e:
        error_message = f"An unexpected error occurred during Fabric API call: {e}"
        st.error(f"Fabric API call failed: {e}")
        raise requests.exceptions.RequestException(error_message) from e
    except json.JSONDecodeError as e:
        error_message = f"JSON Decode Error: Could not parse response from Fabric API. Response: {response.text}. Error: {e}"
        st.error(f"Fabric API call failed: {error_message}")
        raise requests.exceptions.RequestException(error_message) from e


def list_fabric_workspaces(app_instance) -> list[dict]:
    """
    Lists all accessible Fabric workspaces for the authenticated user.
    Returns a list of dictionaries, each with 'id' and 'name'.
    """
    if not app_instance.jwt_token:
        st.error("JWT Token is missing for listing workspaces.")
        return []
    try:
        endpoint = "/v1/workspaces"  # Correct endpoint for listing workspaces
        st.info("Fetching Fabric workspaces")
        response = call_fabric_api(app_instance, "GET", endpoint)
        return response.get('value', [])
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch Fabric workspaces: {e}")
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred while listing Fabric workspaces: {e}")
        return []


def list_onelake_tables(app_instance) -> list[str]:
    """
    Fetches a list of table names from the specified Fabric Lakehouse via live API call
    using the non-schema-aware endpoint.
    """
    if not app_instance.fabric_workspace_id or \
            not app_instance.fabric_lakehouse_id or \
            not app_instance.jwt_token:  # Removed schema name check
        st.error("Fabric Workspace ID, Lakehouse ID, or JWT Token is missing for Onelake table listing.")
        return []

    try:

        endpoint = f"/v1/workspaces/{app_instance.fabric_workspace_id}/lakehouses/{app_instance.fabric_lakehouse_id}/tables"
        # st.info(f"Fetching tables from Lakehouse '{app_instance.fabric_lakehouse_id}'")
        response = call_fabric_api(app_instance, "GET", endpoint)

        table_names = [table['name'] for table in response.get('data', [])]
        return table_names
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch Onelake tables: {e}")
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred while listing Onelake tables: {e}")
        return []


def list_adls_paths(app_instance) -> list[str]:
    """
    Returns a predefined list of ADLS paths as a conceptual representation.
    """
    return None


def upload_file_to_lakehouse_files(app_instance, uploaded_file_object, destination_folder: str = "user_uploads") -> \
        tuple[bool, str]:
    """
    Uploads a file to the Lakehouse's staging area and then moves it to a specified folder
    within the 'Files' section using DataLakeServiceClient.
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

    # Get the authenticated DataLakeServiceClient
    if not app_instance.onelake_token:
        return False, "Failed to get DataLakeServiceClient. Check authentication."

    file_name = uploaded_file_object.name
    file_content = uploaded_file_object.getvalue()  # Get binary content of the uploaded file
    data_path = f"{app_instance.fabric_lakehouse_name}.Lakehouse/Files"

    try:
        file_system_client = app_instance.onelake_token.get_file_system_client(app_instance.fabric_workspace_name)
        directory_client = file_system_client.get_directory_client(data_path)
        file_client = directory_client.create_file(file_name)
        # file_contents = file_obj.read()
        file_client.append_data(data=file_content, offset=0, length=len(file_content))
        file_client.flush_data(len(file_content))
        return True, f'Files/{file_name}'
    except Exception as e:
        return False, f"Upload failed: {e}"


def initiate_load_to_lakehouse_table(app_instance, relative_file_path: str, target_table_name: str, file_format: str,
                                     header: bool = True, delimiter: str = ','):
    """
    Initiates the "Load to Tables" operation in Fabric, converting a file already in 'Files'
    into a Delta table.
    Args:
        app_instance: The PipelineApp instance.
        relative_file_path (str): The path to the file in Lakehouse 'Files' (e.g., 'Files/user_uploads/my_file.csv').
        target_table_name (str): The name of the table to create/load into in the Lakehouse 'Tables' section.
        file_format (str): The format of the file (e.g., 'CSV', 'PARQUET').
        header (bool): True if the file has a header row, False otherwise.
        delimiter (str): The delimiter for CSV files.
    Returns:
        str | None: The URL to poll for operation status on success, None on failure.
    """
    if not app_instance.fabric_workspace_id or \
            not app_instance.fabric_lakehouse_id or \
            not app_instance.jwt_token:
        st.error("Fabric connection details missing for 'Load to Tables' API.")
        return None

    load_endpoint = f"/v1/workspaces/{app_instance.fabric_workspace_id}/lakehouses/{app_instance.fabric_lakehouse_id}/tables/{target_table_name}/load"


    payload = {
        "relativePath": relative_file_path,
        "pathType": "File",
        "mode": "overwrite",
        "formatOptions": {
            "header": str(header).lower(),
            "delimiter": delimiter,
            "format": file_format.upper()
        }
    }

    st.info(f"Initiating 'Load to Tables' for '{relative_file_path}' into table '{target_table_name}'...")
    try:
        response = call_fabric_api(app_instance, "POST", load_endpoint, payload=payload)

        # Check if call_fabric_api returned a 'Location' for polling
        if response.get("status") == "success" and "Location" in response:
            st.success("Load to Tables operation initiated")
                       # f" successfully. Polling URL: {response['Location']}")
            return response['Location']
        else:
            # st.error(f"Failed to 'Load  Tables' operation: {response.get('message', 'Unknown error')}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"'Load to Tables' API call failed: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred during 'Load to Tables' initiation: {e}")
        return None


# def get_fabric_operation_status(app_instance, operation_url: str) -> dict:
#     """
#     Polls the status of an asynchronous Fabric operation (e.g., Load to Tables).
#     Args:
#         app_instance: The PipelineApp instance.
#         operation_url (str): The URL obtained from the 'Location' header of the initial API response.
#     Returns:
#         dict: The JSON response containing the status of the operation.
#     """
#     st.info(f"Polling operation status from: {operation_url}")
#     try:
#         headers = {"Authorization": f"Bearer {app_instance.jwt_token}"}
#         response = requests.get(operation_url, headers=headers)
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         st.error(f"Failed to poll operation status: {e}")
#         return {"status": "Failed", "error": str(e)}
#     except json.JSONDecodeError:
#         st.error(f"Failed to parse JSON response from status URL: {operation_url}. Response: {response.text}")
#         return {"status": "Failed", "error": "Invalid JSON response"}
#     except Exception as e:
#         st.error(f"An unexpected error occurred during status polling: {e}")
#         return {"status": "Failed", "error": str(e)}


def call_fabric_notebook(app_instance, notebook_id: str, notebook_params: dict = None):
    """
    Calls a Fabric Notebook to execute it with optional parameters.
    Args:
        app_instance: The PipelineApp instance.
        notebook_id (str): The ID of the Fabric Notebook to execute.
        notebook_params (dict, optional): Dictionary of parameters to pass to the notebook.
                                          These will be serialized to a JSON string for the 'executionData'.
                                          Defaults to None.
    Returns:
        str | None: The URL to poll for the notebook's execution status on success, None on failure.
    """
    if not app_instance.fabric_workspace_id or not app_instance.jwt_token:
        st.error("Fabric Workspace ID or JWT Token is missing for calling a Fabric Notebook.")
        return None

    # Construct the endpoint for running a notebook instance
    endpoint = f"/v1/workspaces/{app_instance.fabric_workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    payload = {}

    # if notebook_params:
    #     payload["executionData"] = json.dumps(notebook_params)
    # st.write(payload["executionData"])
    st.info(f"Starting notebook `{notebook_id}`...")

    try:
        response = call_fabric_api(app_instance, "POST", endpoint, payload=payload)
        if response.get("status") == "success" and "Location" in response:
            st.success("Notebook execution started!")
            # st.code(response["Location"], language="bash")
            return response["Location"]
        else:
            st.error("Notebook execution did not start successfully.")
            # st.json(response)
    except Exception as e:
        st.error(f"Exception: {e}")
    return None

def poll_notebook_status(app_instance, location_url):
    st.info("Checking notebook execution status")

    while True:
        try:
            poll_count = 0
            max_polls = 12
            status = 'NotStarted'
            st.write(f"Notebook status: **{status}** (Poll {poll_count + 1}/{max_polls})")
            endpoint = location_url.replace("https://api.fabric.microsoft.com", "")
            time.sleep(10)
            response = call_fabric_api(app_instance, "GET", endpoint)
            status = response.get("status")

            poll_count += 1

            if status in ["Completed", "Failed", "Cancelled"]:
                # st.subheader("notebook response:")
                # st.json(response)
                # if output_logs:
                #     st.subheader("Notebook Output")
                #     st.code(output_logs)

                error_details = response.get("error", {}).get("message")
                if error_details:
                    st.subheader("Error Details")
                    st.error(error_details)

                break

            time.sleep(10)
        except Exception as e:
            st.error(f"Error checking notebook status: {e}")
            break
    return

def read_file_from_onelake(app_instance,file_path: str,file_name=None):
    """
    Reads the content of a file from OneLake Files.
    Args:
        app_instance: The PipelineApp instance.
        file_path (str): The full path to the file in OneLake Files (e.g., 'LakehouseName.Lakehouse/Files/temp_data/schema.json').
    Returns:
        str | None: The content of the file as a string, or None if an error occurs.
    """
    if not app_instance.onelake_token:
        st.error("DataLakeServiceClient is not available to read file from OneLake.")
        return None



    try:
        file_system = app_instance.onelake_token.get_file_system_client(app_instance.fabric_workspace_name)
        directory_client = file_system.get_directory_client(file_path)
        paths = list(directory_client.get_paths())
        # st.write(paths)
        file_names = [p.name.split("/")[-1] for p in paths if not p.is_directory and p.name.split(".")[-1]!='crc']
        file_names_dict = {name: name for name in file_names}
        if not file_names:
            st.warning("No files found in folder.")
        else:
            # selected_file = st.selectbox("Select file to load", file_names)
            selected_file=file_names_dict[file_name]
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
                        # app_instance.current_df = preview_df
                        st.subheader("Top 5 Rows")
                        st.dataframe(preview_df, use_container_width=True)

                    # st.download_button(" Download File", data=data, file_name=selected_file)
                    file_client.delete_file()
                    st.success(f"File '{selected_file}' read and deleted from OneLake.")
                except Exception as e:
                    st.error(f"Failed to load file: {e}")
    except Exception as e:
        st.error(f"Failed to list files: {e}")
    return preview_df


# def delete_file_from_onelake(app_instance,file_path: str) -> bool:
#     """
#     Deletes a file from OneLake Files.
#     Args:
#         app_instance: The PipelineApp instance.
#         file_path (str): The full path to the file in OneLake Files.
#     Returns:
#         bool: True if deletion was successful, False otherwise.
#     """
#     if not app_instance.onelake_token:
#         st.error("DataLakeServiceClient is not available to delete file from OneLake.")
#         return False
#
#     try:
#         file_system_client = app_instance.onelake_token.get_file_system_client(app_instance.fabric_workspace_id)
#         file_client = file_system_client.get_file_client(file_path)
#         file_client.delete_file()
#         st.success(f"Successfully deleted temporary file from OneLake: {file_path}")
#         return True
#     except Exception as e:
#         st.error(f"Failed to delete file '{file_path}' from OneLake: {e}")
#         return False