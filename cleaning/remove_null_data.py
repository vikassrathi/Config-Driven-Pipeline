import pandas as pd
from cleaning.base_cleaning import BaseCleaning  # Changed import
import json # Import json for serializing lists to parameters


class RemoveNullData(BaseCleaning):  # Changed base class
    """
    A cleaning operation to remove rows or columns containing missing values (NaN) in a DataFrame.
    """

    def __init__(self, app_instance, utils_instance, add_lineage_step_func,
                 strategy: str,
                 columns_to_check: list = None,
                 column_to_drop: str = None
                 ):
        super().__init__(app_instance, utils_instance, add_lineage_step_func)
        self.strategy = strategy
        self.columns_to_check = columns_to_check  # Specific columns to check for nulls when dropping rows
        self.column_to_drop = column_to_drop  # Single column to drop if it has nulls
        self.config = {}  # Initialize config for lineage/YAML

    def apply_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the null removal strategy to the DataFrame.
        """
        df_copy = df.copy()
        initial_rows = len(df_copy)
        initial_cols = len(df_copy.columns)

        if self.strategy == 'drop_rows_col' and self.columns_to_check:
            # Ensure all columns to check actually exist in the DataFrame
            valid_columns = [col for col in self.columns_to_check if col in df_copy.columns]
            if not valid_columns:
                self.app_instance.st.warning(f"No valid columns selected for dropping rows based on nulls. Skipping.")
                self.config = {"status": "skipped", "reason": "No valid columns for drop_rows_col."}
                return df_copy

            df_copy.dropna(subset=valid_columns, inplace=True)
            rows_dropped = initial_rows - len(df_copy)
            self.config = {"strategy": self.strategy, "columns_checked": valid_columns, "rows_dropped": rows_dropped}
            self.app_instance.st.info(
                f"Dropped {rows_dropped} rows with nulls in specified columns: {', '.join(valid_columns)}.")

        elif self.strategy == 'drop_rows_any':
            df_copy.dropna(inplace=True)  # Drops rows with any NaN across all columns
            rows_dropped = initial_rows - len(df_copy)
            self.config = {"strategy": self.strategy, "scope": "any_column", "rows_dropped": rows_dropped}
            self.app_instance.st.info(f"Dropped {rows_dropped} rows with nulls in any column.")

        elif self.strategy == 'drop_column' and self.column_to_drop:
            if self.column_to_drop not in df_copy.columns:
                self.app_instance.st.warning(f"Column '{self.column_to_drop}' not found for dropping. Skipping.")
                self.config = {"status": "skipped", "reason": f"Column '{self.column_to_drop}' not found."}
                return df_copy

            if df_copy[self.column_to_drop].isnull().any():  # Only drop if it actually has nulls
                df_copy.drop(columns=[self.column_to_drop], inplace=True)
                cols_dropped = initial_cols - len(df_copy.columns)
                self.config = {"strategy": self.strategy, "column_dropped": self.column_to_drop,
                               "cols_dropped_count": cols_dropped}
                self.app_instance.st.info(f"Dropped column '{self.column_to_drop}' due to nulls.")
            else:
                self.app_instance.st.info(f"Column '{self.column_to_drop}' has no nulls. Not dropped.")
                self.config = {"status": "skipped", "reason": f"Column '{self.column_to_drop}' has no nulls."}

        return df_copy

    def get_display_name(self) -> str:
        """
        Returns a user-friendly name for the null removal step.
        """
        if self.strategy == 'drop_rows_col':
            return f"Drop Rows with Nulls in {self.columns_to_check}"
        elif self.strategy == 'drop_rows_any':
            return "Drop Rows with Nulls in Any Column"
        elif self.strategy == 'drop_column':
            return f"Drop Column '{self.column_to_drop}' (if nulls present)"
        return f"Unknown Null Removal Strategy: {self.strategy}"

    def get_config_for_yaml(self) -> dict:
        """
        Returns a dictionary representation of the cleaning step's configuration
        suitable for serialization into a YAML file for Fabric.
        """
        config = {
            "type": "remove_null_data",
            "strategy": self.strategy
        }
        if self.columns_to_check:
            config["columns_to_check"] = self.columns_to_check
        if self.column_to_drop:
            config["column_to_drop"] = self.column_to_drop
        return config

    # --- Fabric Integration Placeholders (NEW) ---
    def get_parameters(self) -> dict:
        """
        Returns parameters for Fabric Notebook execution.
        """
        params = {
            "strategy": {"value": str(self.strategy), "type": "str"}
        }
        if self.columns_to_check:
            params["columns_to_check"] = {"value": json.dumps(self.columns_to_check),
                                          "type": "str"}  # JSON string for list
        if self.column_to_drop:
            params["column_to_drop"] = {"value": str(self.column_to_drop), "type": "str"}
        return params

    def get_fabric_notebook_id(self) -> str:
        """
        Returns the ID of the Fabric Notebook responsible for this cleaning operation.
        (Placeholder for actual Fabric Notebook ID)
        """
        return "fabric_notebook_id_for_remove_nulls"

    def get_fabric_dataflow_activity_json(self) -> dict:
        """
        Returns the JSON structure for a Fabric Dataflow activity that performs
        this cleaning step. (Placeholder for actual Fabric Dataflow activity JSON)
        """
        return {
            "name": f"RemoveNulls_{self.strategy}",
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
        (Placeholder - expand with actual validation logic)
        """
        #  ensure strategy is set, and columns if required by strategy
        if self.strategy in ['drop_rows_col'] and not self.columns_to_check:
            return False
        if self.strategy == 'drop_column' and not self.column_to_drop:
            return False
        return bool(self.strategy)

