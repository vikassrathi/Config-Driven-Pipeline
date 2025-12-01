import pandas as pd
from cleaning.base_cleaning import BaseCleaning
import json


class DropColumnsCleaning(BaseCleaning):
    """
    Concrete implementation for dropping specified columns as a cleaning step.
    """

    def __init__(self, app_instance, utils_module, add_lineage_step_func, columns_to_drop: list):
        super().__init__(app_instance, utils_module, add_lineage_step_func)
        self.columns_to_drop = columns_to_drop
        self.config = {}  # Initialize config for lineage/YAML

    def apply_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the column dropping logic to the DataFrame.
        """
        df_copy = df.copy()
        initial_cols = df_copy.columns.tolist()
        dropped_cols = []

        for col in self.columns_to_drop:
            if col in df_copy.columns:
                df_copy.drop(columns=[col], inplace=True)
                dropped_cols.append(col)
            else:
                self.app_instance.st.warning(f"Column '{col}' not found for dropping. Skipping.")

        self.config = {"columns_dropped": dropped_cols, "columns_not_found": list(set(self.columns_to_drop) - set(dropped_cols))}
        if dropped_cols:
            self.app_instance.st.info(f"Dropped columns: {', '.join(dropped_cols)}.")
        else:
            self.app_instance.st.info("No specified columns were found or dropped.")
        return df_copy

    def get_display_name(self) -> str:
        """
        Returns a user-friendly name for the column dropping step.
        """
        return f"Drop Columns: {', '.join(self.columns_to_drop)}"

    def get_config_for_yaml(self) -> dict:
        """
        Returns a dictionary representation of the cleaning step's configuration
        suitable for serialization into a YAML file for Fabric.
        """
        return {
            "type": "drop_columns",
            "columns_to_drop": self.columns_to_drop
        }

    def get_parameters(self) -> dict:
        """
        Returns parameters for Fabric Notebook execution.
        """
        return {
            "columns_to_drop": {"value": json.dumps(self.columns_to_drop), "type": "str"}
        }

    def get_fabric_notebook_id(self) -> str:
        """
        Returns the ID of the Fabric Notebook responsible for this cleaning operation.
        """
        return "fabric_notebook_id_for_drop_columns"  # Placeholder

    def get_fabric_dataflow_activity_json(self) -> dict:
        """
        Returns the JSON structure for a Fabric Dataflow activity.
        """
        return {
            "name": f"DropCols_{len(self.columns_to_drop)}",
            "type": "Notebook",
            "dependsOn": [],
            "typeProperties": {
                "notebookId": self.get_fabric_notebook_id(),
                "parameters": self.get_parameters(),
            }
        }

    def is_valid_for_pipeline(self) -> bool:
        """
        Checks if the cleaning step is configured correctly for Fabric pipeline inclusion.
        """
        return bool(self.columns_to_drop)

