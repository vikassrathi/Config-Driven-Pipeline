from data_sources.abstract_data_source import AbstractDataSource
import utils


class AdlsDataSource(AbstractDataSource):
    """
    Concrete implementation for the ADLS data source.
    Handles data retrieval and internal state management for ADLS without direct UI rendering.
    (Conceptual for now, full ADLS connectivity would require more advanced APIs/SDKs)
    """

    def get_source_options(self):
        """
        Returns a predefined list of ADLS paths/files.
        This remains conceptual as direct dynamic listing of arbitrary ADLS paths
        via general Fabric APIs is complex without dedicated ADLS APIs/SDKs.
        """
        return utils.list_adls_paths(self.app_instance)

    def set_active_selection(self, selected_option_value):
        """
        Sets the active selected ADLS path/file in app_instance based on the provided value.
        This method updates the application's core state without UI.
        """
        self.app_instance.selected_table = selected_option_value
        self.app_instance.selected_table_is_local = False
        self.app_instance.current_df = None

        if selected_option_value:
            self.add_lineage_step(
                self.app_instance,
                "Data Source Selected",
                details={"source_type": "ADLS", "path": selected_option_value}
            )

    def is_selection_valid(self):
        """
        Checks if a valid ADLS path/file has been selected.
        Returns True if a path/file is selected in app_instance.selected_table, False otherwise.
        """
        return self.app_instance.selected_table is not None and self.app_instance.selected_table != ""

    def reset_source_state(self):
        """
        Resets the relevant ADLS-specific state in app_instance.
        """
        self.app_instance.table_data = {}  # Clear table data cache
        self.app_instance.uploaded_file_object = None  # Clear local file
        self.app_instance.uploaded_df = None
        self.app_instance.selected_table_is_local = False
        self.app_instance.current_df = None
        self.app_instance.selected_table = None

