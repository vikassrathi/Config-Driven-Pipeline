from abc import ABC, abstractmethod
import pandas as pd


class BaseCleaning(ABC):
    """
    Abstract Base Class for data cleaning implementations.
    Defines the interface for cleaning logic and Fabric payload generation.
    """

    def __init__(self, app_instance, utils_module, add_lineage_step_func):
        """
        Initializes the cleaning step with a reference to the main app instance,
        the utilities module, the lineage tracking function
        """
        self.app_instance = app_instance
        self.utils = utils_module
        self.add_lineage_step = add_lineage_step_func

    @abstractmethod
    def apply_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the specific cleaning logic to the DataFrame.
        Must be implemented by concrete classes.
        """
        pass

    @abstractmethod
    def get_display_name(self) -> str:
        """
        Returns a human-readable name for the cleaning step.
        """
        pass

    @abstractmethod
    def get_config_for_yaml(self) -> dict:
        """
        Returns a dictionary representation of the cleaning step's configuration
        suitable for serialization into a YAML file for Fabric.
        """
        pass

    @abstractmethod
    def get_parameters(self) -> dict:
        """
        Returns a dictionary of parameters specific to this cleaning step,
        formatted for Fabric Notebooks.
        Example: {"remove_nulls": {"value": "True", "type": "bool"}}
        """
        pass

    @abstractmethod
    def get_fabric_notebook_id(self) -> str:
        """
        Returns the Fabric Notebook ID associated with this cleaning step.
        """
        pass

    @abstractmethod
    def get_fabric_dataflow_activity_json(self) -> dict:
        """
        Returns the JSON structure for a Fabric Dataflow activity that performs
        this cleaning step.
        """
        pass


    @abstractmethod
    def is_valid_for_pipeline(self) -> bool:
        """
        Checks if the cleaning step is configured correctly and valid for inclusion
        in the Fabric pipeline payload.
        """
        pass

    def add_lineage(self, details=None):
        """
        Adds a lineage step for this cleaning operation.
        """
        display_name = self.get_display_name()
        self.add_lineage_step(self.app_instance, f"Cleaning: {display_name}", details)
