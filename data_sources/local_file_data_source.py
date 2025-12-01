import pandas as pd
from data_sources.abstract_data_source import AbstractDataSource


class LocalFileDataSource(AbstractDataSource):
    """
    Concrete implementation for the Local File data source.
    Handles reading the uploaded file into a DataFrame and internal state management.
    """

    def get_source_options(self):
        """
        For a local file source, there are no predefined "options" to get.
        This method returns None or an empty list as the UI will handle file upload directly.
        """
        return None

    def set_active_selection(self, uploaded_file_object, target_table_name: str = None,
                             file_format_options: dict = None):
        """
        Processes the uploaded file object, reads it into a DataFrame,
        and updates the app_instance state.
        Args:
            uploaded_file_object: The Streamlit UploadedFile object.
            target_table_name (str): The desired name for the Lakehouse table (from UI).
            file_format_options (dict): Dictionary containing 'format', 'header', 'delimiter' (from UI).
        Returns:
            bool: True if a new file was processed or state updated, False if it was the same file already processed.
        """
        if uploaded_file_object is not None:
            current_file_id = (uploaded_file_object.name, uploaded_file_object.size)
            if self.app_instance.uploaded_file_object is None or \
               current_file_id != (self.app_instance.uploaded_file_object.name, self.app_instance.uploaded_file_object.size) or \
               self.app_instance.target_lakehouse_table_name != target_table_name or \
               self.app_instance.uploaded_file_format_options != file_format_options:

                self.app_instance.uploaded_file_object = uploaded_file_object
                self.app_instance.target_lakehouse_table_name = target_table_name
                self.app_instance.uploaded_file_format_options = file_format_options

                file_format = file_format_options.get("format")
                has_header = file_format_options.get("header", True)
                delimiter = file_format_options.get("delimiter", ",")

                try:
                    self.app_instance.uploaded_file_object.seek(0)

                    if file_format == "CSV":
                        df = pd.read_csv(self.app_instance.uploaded_file_object,
                                         header=0 if has_header else None,
                                         delimiter=delimiter)
                    elif file_format == "xlsx":
                        df = pd.read_excel(self.app_instance.uploaded_file_object,
                                           header=0 if has_header else None)
                    else:
                        self.app_instance.st.error("Unsupported file type specified for reading: " + file_format)
                        self.app_instance.current_df = None
                        return False # Indicate failure to

                    self.app_instance.current_df = df
                    self.app_instance.selected_table = uploaded_file_object.name  # Store original file name as selected table
                    self.app_instance.selected_table_is_local = True

                    # lineage step
                    self.add_lineage_step(
                        self.app_instance,
                        "Local File Loaded",
                        details={"file_name": uploaded_file_object.name, "format": file_format,
                                 "rows": df.shape[0], "cols": df.shape[1]}
                    )
                    # Reset Fabric upload status when a new local file is processed
                    self.app_instance.is_file_uploaded_to_lakehouse = False
                    return True # Indicate successful processing
                except Exception as e:
                    self.app_instance.st.error(f"Error reading uploaded file: {e}")
                    self.app_instance.current_df = None
                    self.app_instance.uploaded_file_object = None # Clear object on error
                    self.app_instance.is_file_uploaded_to_lakehouse = False
                    return False # Indicate failure to
            else:
                return False
        else:
            self.app_instance.uploaded_file_object = None # Ensure it's None if no file
            self.app_instance.current_df = None
            self.app_instance.selected_table_is_local = False
            self.app_instance.is_file_uploaded_to_lakehouse = False
            return False


    def is_selection_valid(self):
        """
        Checks if a valid local file has been uploaded, read into a DataFrame,
        a Lakehouse table name has been provided, AND if the file has been
        successfully uploaded and loaded into a Lakehouse table.
        """
        return True
        # return (self.app_instance.current_df is not None and
        #         not self.app_instance.current_df.empty and
        #         self.app_instance.target_lakehouse_table_name is not None and
        #         self.app_instance.target_lakehouse_table_name != "" and
        #         hasattr(self.app_instance, 'is_file_uploaded_to_lakehouse') and  # Ensure attribute exists
        #         self.app_instance.is_file_uploaded_to_lakehouse)  # Check Fabric upload status

    def reset_source_state(self):
        """
        Resets the relevant local file-specific state in app_instance.
        """
        self.app_instance.uploaded_file_object = None
        self.app_instance.uploaded_df = None
        self.app_instance.selected_table_is_local = False
        self.app_instance.current_df = None
        self.app_instance.selected_table = None
        self.app_instance.table_data = {}
        self.app_instance.target_lakehouse_table_name = ""
        # self.app_instance.uploaded_file_fabric_relative_path = ""
        self.app_instance.uploaded_file_format_options = {}
        # Reset this flag and the last uploaded file key when state is reset
        self.app_instance.is_file_uploaded_to_lakehouse = False
        self.app_instance.last_uploaded_fabric_file_key = None # re-upload logic
