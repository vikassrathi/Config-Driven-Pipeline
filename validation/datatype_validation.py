import pandas as pd
from validation.abstract_validation import AbstractValidation


class DataTypeValidation(AbstractValidation):
    """
    Concrete implementation for Data Type Validation, generating a Great Expectations
    `expect_column_values_to_be_of_type` expectation.
    """

    def __init__(self, app_instance, utils_module, add_lineage_step_func, column: str, expected_type: str):
        super().__init__(app_instance, utils_module, add_lineage_step_func)
        self.column_to_check = column
        self.expected_type = expected_type  # GE type string (e.g., "string", "int", "float", "boolean")
        self.display_name_str = f"Data Type: {column} as {expected_type}"

    def get_display_name(self) -> str:
        return self.display_name_str

    def get_ge_expectation_config(self) -> dict:
        """
        Returns the Great Expectations configuration for 'expect_column_values_to_be_of_type'.
        """
        return {
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {"column": self.column_to_check, "type": self.expected_type}
        }

    # These methods are for Fabric deployment config and are placeholders for now
    def get_fabric_notebook_id(self) -> str:
        return "your_datatype_validation_notebook_id"  # Placeholder

    def get_fabric_dataflow_activity_json(self) -> dict:
        return {
            "type": "Dataflow",
            "name": "Dataflow_DataTypeValidation",
            "typeProperties": {
                "dataflow": {"id": "your_dataflow_id_for_datatype_validation"},
                "compute": {"coreCount": 8, "computeType": "General"},
                "traceLevel": "Fine",
                "parameters": {
                    "column_name": {"value": self.column_to_check, "type": "string"},
                    "expected_type": {"value": self.expected_type, "type": "string"}
                }
            }
        }

    def is_valid_for_pipeline(self) -> bool:
        # Valid if the column exists in the current DataFrame
        return self.column_to_check in self.app_instance.current_df.columns
