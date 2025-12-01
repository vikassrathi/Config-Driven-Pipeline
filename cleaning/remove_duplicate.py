import pandas as pd
from cleaning.base_cleaning import BaseCleaning


class RemoveDuplicatesCleaning(BaseCleaning):
    """
    Concrete implementation for removing duplicate rows as a cleaning step.
    """

    def __init__(self, app_instance, utils_module, add_lineage_step_func, ):
        super().__init__(app_instance, utils_module, add_lineage_step_func)
        self.display_name_str = "Remove Duplicates"
        self.config = {}

    def apply_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the duplicate removal logic to the DataFrame.
        """
        df_copy = df.copy()
        initial_rows = len(df_copy)
        df_copy.drop_duplicates(inplace=True)
        rows_removed = initial_rows - len(df_copy)
        self.config = {"rows_removed": rows_removed}
        self.app_instance.st.info(f"Removed {rows_removed} duplicate rows.")
        return df_copy


    def get_display_name(self) -> str:
        return self.display_name_str

    def get_config_for_yaml(self) -> dict:
        """
        Returns a dictionary representation of the cleaning step's configuration
        suitable for serialization into a YAML file for Fabric.
        """
        return {
            "type": "remove_duplicates"
        }

    def get_parameters(self) -> dict:
        # Notebook will handle 'remove_duplicates' as a boolean flag
        return {"remove_duplicates": {"value": "True", "type": "bool"}}

    def get_fabric_notebook_id(self) -> str:
        return "fabric_notebook_id_for_remove_duplicates" # Placeholder

    def get_fabric_dataflow_activity_json(self) -> dict:
        return {
            "name": self.get_display_name().replace(" ", ""),
            "type": "Notebook",
            "dependsOn": [],
            "typeProperties": {
                "notebookId": self.get_fabric_notebook_id(),
                "parameters": self.get_parameters(),
            }
        }


    def is_valid_for_pipeline(self) -> bool:
        return True

