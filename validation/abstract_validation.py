from abc import ABC, abstractmethod
import pandas as pd


class AbstractValidation(ABC):
    """
    Abstract Base Class for data validation implementations.
    Defines the interface for building Great Expectations configurations
    and Fabric payload generation.
    Concrete implementations should NOT contain Streamlit UI calls.
    """

    def __init__(self, app_instance, utils_module, add_lineage_step_func):
        """
        Initializes the validation step with a reference to the main app instance,
        the utilities module, and the lineage tracking function.
        """
        self.app_instance = app_instance
        self.utils = utils_module
        self.add_lineage_step = add_lineage_step_func

    @abstractmethod
    def get_display_name(self) -> str:
        """
        Returns a human-readable name for the validation rule.
        """
        pass

    @abstractmethod
    def get_ge_expectation_config(self) -> dict:
        """
        Returns a dictionary representing the Great Expectations ExpectationConfiguration
        for this specific validation rule.
        Example: {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "some_col"}}
        """
        pass

    # The methods below are primarily for Fabric deployment config, not local execution logic
    @abstractmethod
    def get_fabric_notebook_id(self) -> str:
        """
        Returns the Fabric Notebook ID associated with this validation rule.
        """
        pass

    @abstractmethod
    def get_fabric_dataflow_activity_json(self) -> dict:
        """
        Returns the JSON structure for a Fabric Dataflow activity that performs
        this validation rule.
        """
        pass

    @abstractmethod
    def is_valid_for_pipeline(self) -> bool:
        """
        Checks if the validation rule is configured correctly and valid for inclusion
        in the Fabric pipeline payload. This check will often involve current_df.
        """
        pass

    def add_lineage(self, details=None):
        """
        Adds a lineage step for this validation operation.
        """
        display_name = self.get_display_name()
        self.add_lineage_step(self.app_instance, f"Validation: {display_name}", details)
