
# Streamlit's global state (st.session_state) is accessed via app_instance attributes.
from data_sources.abstract_data_source import AbstractDataSource

import utils
import streamlit as st
import pandas as pd
import json
import time




class OnelakeDataSource(AbstractDataSource):
    """
    Concrete implementation for the Onelake data source.
    Handles data retrieval and internal state management for Onelake without direct UI rendering.
    Now uses the non-schema-aware API for live table listing.
    """

    def get_source_options(self):
        """
        Fetches and returns a list of table names from the specified Fabric Lakehouse
        via live API call using the non-schema-aware endpoint.
        """
        if not self.app_instance.fabric_workspace_id or \
                not self.app_instance.fabric_lakehouse_id or \
                not self.app_instance.jwt_token:  # Removed schema name check
            self.app_instance.st.warning(
                "Fabric connection details (Workspace ID, Lakehouse ID, or JWT Token) not fully set. Cannot list Onelake tables.")
            return []

        try:
            table_names = utils.list_onelake_tables(self.app_instance)

            if not table_names:
                self.app_instance.st.info(
                    f"No tables found in Lakehouse '{self.app_instance.fabric_lakehouse_id}'.")
            return table_names
        except Exception as e:
            self.app_instance.st.error(f"Failed to list Onelake tables: {e}")
            return []

    def set_active_selection(self, selected_option_value):
        """
        Sets the active selected Onelake table in app_instance based on the provided value.
        This method updates the application's core state without UI.
        For Onelake, this now triggers a Fabric Notebook to get schema and sample data.
        """
        self.app_instance.selected_table = selected_option_value
        self.app_instance.selected_table_is_local = False
        self.app_instance.current_df = None  # Clear previous DataFrame
        self.app_instance.selected_table_schema = None  # Clear previous schema

        # Log lineage step
        if selected_option_value:
        #     self.add_lineage_step(  # Use the injected lineage function
        #         self.app_instance,
        #         "Data Source Selected",
        #         details={"source_type": "Onelake", "table_name": selected_option_value}
        #     )

            # --- Trigger Fabric Notebook
            notebook_id = "7d30a52b-4cd6-450c-b788-a734b226362b"

            if not hasattr(self.app_instance, 'session_id') or not self.app_instance.session_id:
                self.app_instance.session_id = str(pd.Timestamp.now().timestamp()).replace(".", "")  # Simple unique ID

            # notebook_params = {
            #     "lakehouse_id": self.app_instance.fabric_lakehouse_id,
            #     "table_name": selected_option_value,
            #     "output_base_path": f"Files/temp_data"
            #                         # f"{self.app_instance.session_id}/{selected_option_value}"
            # }

            st.info(
                f"Fetching schema and data for '{selected_option_value}' from Onelake")

            operation_url = None
            try:
                with st.spinner("Initiating Fabric Notebook to fetch table"):
                    operation_url = utils.call_fabric_notebook(self.app_instance, notebook_id)
            except Exception as e:
                st.error(f"Failed to initiate Fabric Notebook: {e}")
                return

            if operation_url:
                with st.status("Polling Notebook status for table info", expanded=True) as status_box:
                    status = {"status":''}
                    # status_box.write(f"Notebook status: {status['status']}")
                    time.sleep(5)
                    response = utils.poll_notebook_status(self.app_instance, operation_url)
                    status['status'] = response.get("status")
                        # st.write(response)
                    if status["status"] == "Completed":
                        status_box.update(label="Fabric Notebook completed successfully!", state="complete",
                                          expanded=False)


                        schema_file_path = "selfserving.Lakehouse/Files/temp_data"
                        file_name='sample.csv'
                        sample_file_path = "https://onelake.dfs.fabric.microsoft.com/5c794792-12f8-4e81-86a5-784ececf5b16/c634d84b-dac5-4891-ac33-958a4e6444fc.Lakehouse/Files/temp_data/sample.csv"
                        if schema_file_path and sample_file_path:
                                    st.info('Reading the sample file')
                                    if self.app_instance.onelake_token and self.app_instance.current_df is None:
                                        self.app_instance.current_df=utils.read_file_from_onelake(self.app_instance,
                                                                                      schema_file_path,file_name)
                                        # self.app_instance.selected_table_schema=self.app_instance.current_df.dtypes
                                        self.app_instance.selected_table_schema = [
                                            {"name": col, "type": str(dtype)}
                                            for col, dtype in self.app_instance.current_df.dtypes.items()
                                        ]
                                        # sample_content = utils.read_file_from_onelake(self.app_instance,
                                        #                                               sample_file_path)

                                        # if schema_content and sample_content:
                                        #     self.app_instance.selected_table_schema = json.loads(schema_content)
                                        #     # Assuming sample is CSV, read it into DataFrame
                                        #     from io import StringIO
                                        #     self.app_instance.current_df = pd.read_csv(StringIO(sample_content))
                                        #     st.success("Schema and sample data fetched successfully!")
                                            # st.dataframe(self.app_instance.current_df.head()) # Displayed in select_data_source.py

                                            # Add lineage for schema/sample fetch
                                            # self.add_lineage_step(
                                            #     self.app_instance,
                                            #     "Onelake Table Info Fetched",
                                            #     details={"table_name": selected_option_value,
                                            #              "schema_cols": len(self.app_instance.selected_table_schema)}
                                            # )

                                            # Clean up temporary files
                                            # utils.delete_file_from_onelake(self.app_instance, self.app_instance.onelake_token,
                                            #                                schema_file_path)
                                            # utils.delete_file_from_onelake(self.app_instance, self.app_instance.onelake_token,
                                            #                                sample_file_path)
                                            # st.info("Temporary files cleaned up from OneLake.")
                                        # else:
                                        #     st.error("Failed to read schema or sample content from OneLake.")
                                    else:
                                        st.error("Failed to get DataLakeServiceClient for reading temporary files.")
                        else:
                                st.error("Notebook output did not contain valid paths for schema or sample files.")
                    elif status["status"] == "Failed":
                        # error_msg = status.get('error', 'Unknown error. Check Fabric logs for details.')
                        status_box.update(label=f"Fabric Notebook failed:", state="error", expanded=True)

                    else:
                        status_box.update(
                            label=f"Notebook status: {status['status']}. Please check Fabric for final status.",
                            state="warning", expanded=True)

            else:
                st.error("Failed to initiate Fabric Notebook. No operation URL received.")

    def is_selection_valid(self):
        """
        Checks if a valid Onelake table has been selected and its schema/sample data
        has been successfully loaded into app_instance.current_df.
        """
        # st.write(self.app_instance.current_df,self.app_instance.selected_table)
        # return (self.app_instance.selected_table is not None and
        #         self.app_instance.selected_table != "" and
        #         self.app_instance.current_df is not None and
        #         not self.app_instance.current_df.empty)
                # and self.app_instance.selected_table_schema is not None)
        return (self.app_instance.selected_table is not None and self.app_instance.current_df is not None)

    def reset_source_state(self):
        """
        Resets the relevant Onelake-specific state in app_instance.
        """
        self.app_instance.table_data = {}
        self.app_instance.uploaded_file_object = None
        self.app_instance.uploaded_df = None
        self.app_instance.selected_table_is_local = False
        self.app_instance.current_df = None
        self.app_instance.selected_table = None
        self.app_instance.selected_table_schema = None
