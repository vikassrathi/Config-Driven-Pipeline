from abc import ABC, abstractmethod
import pandas as pd


class AbstractDataSource(ABC):
    """
    Abstract Base Class for data source implementations.
    Defines the interface for data source connectivity and data retrieval/processing logic.
    """

    def __init__(self, app_instance, utils_module, add_lineage_step_func):
        """
        Initializes the data source with a reference to the main app instance,
        the utilities module, and the lineage tracking function.
        """
        self.app_instance = app_instance
        self.utils = utils_module
        self.add_lineage_step = add_lineage_step_func

    @abstractmethod
    def get_source_options(self):
        """
        Abstract method to retrieve and return a list of options available for selection
        from this data source
        This method should perform data fetching/discovery without rendering UI.
        Returns a list or relevant data structure.
        """
        pass

    @abstractmethod
    def set_active_selection(self, selected_option_value):
        """
        Abstract method to process a user's selection from the source options.
        It should update internal state of the data source and relevant app_instance attributes.
        """
        pass

    @abstractmethod
    def is_selection_valid(self):
        """
        Abstract method to check if a valid selection has been made for this data source.
        Returns True if the current selection state is valid for progression, False otherwise.
        """
        pass

    @abstractmethod
    def reset_source_state(self):
        """
        Abstract method to reset any source-specific state or attributes in app_instance
        when switching to a different data source type or starting anew.
        """
        pass
