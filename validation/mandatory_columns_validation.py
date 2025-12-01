import pandas as pd
from validation.abstract_validation import AbstractValidation


class MandatoryColumnsValidation(AbstractValidation):
    """
    Concrete implementation for Mandatory Columns Validation, generating a Great Expectations
    `expect_column_to_exist` expectation for each mandatory column.
    """

    def __init__(self, app_instance, utils_module, add_lineage_step_func, column: str):  # Takes a single column
        super().__init__(app_instance, utils_module, add_lineage_step_func)
        self.column_to_exist = column
        self.display_name_str = "ExpectColumnToExist"

    def get_display_name(self) -> str:
        return self.display_name_str

    def get_ge_expectation_config(self) -> dict:
        """
        Returns the Great Expectations configuration for 'expect_column_to_exist'.
        """
        return {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": self.column_to_exist}
        }

    # These methods are for Fabric deployment config and are placeholders for now
    def get_fabric_notebook_id(self) -> str:
        return "your_mandatory_columns_notebook_id"  # Placeholder

    def get_fabric_dataflow_activity_json(self) -> dict:
        return {
            "type": "Dataflow",
            "name": "Dataflow_MandatoryColumnValidation",
            "typeProperties": {
                "dataflow": {"id": "your_dataflow_id_for_mandatory_column_validation"},
                "compute": {"coreCount": 8, "computeType": "General"},
                "traceLevel": "Fine",
                "parameters": {
                    "column_name": {"value": self.column_to_exist, "type": "string"}
                }
            }
        }

    def is_valid_for_pipeline(self) -> bool:
        # Valid if the column exists in the current DataFrame
        return self.column_to_exist in self.app_instance.current_df.columns
