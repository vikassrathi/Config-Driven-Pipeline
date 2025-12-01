import pandas as pd
from cleaning.base_cleaning import BaseCleaning
import numpy as np # Import numpy for NaN check


class FillNullData(BaseCleaning):
    """
    A cleaning operation to fill missing values (NaN) in a DataFrame.
    Supports filling with mean/median/specific value, or 'UNKNOWN' for strings.
    """

    def __init__(self, app_instance, utils_instance, add_lineage_step_func,
                 column: str,
                 strategy: str,
                 fill_value: any = None
                 ):
        super().__init__(app_instance, utils_instance, add_lineage_step_func)
        self.column = column
        self.strategy = strategy
        self.fill_value = fill_value
        self.config = {}

    def apply_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the null filling strategy to the specified column.
        """
        df_copy = df.copy()

        if self.column not in df_copy.columns:
            self.app_instance.st.warning(f"Column '{self.column}' not found for null filling. Skipping.")
            self.config = {"status": "skipped", "reason": f"Column '{self.column}' not found."}
            return df_copy

        # Check if the column actually has nulls before proceeding
        if not df_copy[self.column].isnull().any():
            self.app_instance.st.info(f"Column '{self.column}' has no null values. Skipping fill operation.")
            self.config = {"status": "skipped", "reason": f"Column '{self.column}' has no null values."}
            return df_copy


        if self.strategy == 'fill_mean':
            if pd.api.types.is_numeric_dtype(df_copy[self.column]):
                fill_val = df_copy[self.column].mean()
                df_copy[self.column] = df_copy[self.column].fillna(fill_val)
                self.config = {"column": self.column, "strategy": self.strategy, "filled_value_actual": fill_val}
            else:
                self.app_instance.st.warning(f"Cannot fill mean for non-numeric column '{self.column}'. Skipping.")
                self.config = {"status": "skipped",
                               "reason": f"Cannot fill mean for non-numeric column '{self.column}'."}

        elif self.strategy == 'fill_median':
            if pd.api.types.is_numeric_dtype(df_copy[self.column]):
                fill_val = df_copy[self.column].median()
                df_copy[self.column] = df_copy[self.column].fillna(fill_val)
                self.config = {"column": self.column, "strategy": self.strategy, "filled_value_actual": fill_val}
            else:
                self.app_instance.st.warning(f"Cannot fill median for non-numeric column '{self.column}'. Skipping.")
                self.config = {"status": "skipped",
                               "reason": f"Cannot fill median for non-numeric column '{self.column}'."}

        elif self.strategy == 'fill_mode':
            # Mode can be used for both numeric and non-numeric
            if not df_copy[self.column].mode().empty:
                fill_val = df_copy[self.column].mode()[0] # Get the first mode if multiple
                df_copy[self.column] = df_copy[self.column].fillna(fill_val)
                self.config = {"column": self.column, "strategy": self.strategy, "filled_value_actual": fill_val}
            else:
                self.app_instance.st.warning(f"Could not determine mode for column '{self.column}'. Skipping.")
                self.config = {"status": "skipped", "reason": f"Could not determine mode for column '{self.column}'."}

        elif self.strategy == 'fill_specific':
            if self.fill_value is not None:
                # Replace NaN, None, and empty strings (if applicable)
                # For numbers, fillna handles NaN. For strings, also consider empty strings as "missing" for consistency
                if pd.api.types.is_string_dtype(df_copy[self.column]):
                    df_copy[self.column] = df_copy[self.column].replace('', np.nan).fillna(self.fill_value)
                else:
                    df_copy[self.column] = df_copy[self.column].fillna(self.fill_value)
                self.config = {"column": self.column, "strategy": self.strategy, "fill_value": self.fill_value}
            else:
                self.app_instance.st.warning(f"Fill value not provided for specific fill strategy. Skipping.")
                self.config = {"status": "skipped", "reason": "Fill value not provided for specific fill strategy."}

        return df_copy

    def get_display_name(self) -> str:
        """
        Returns a user-friendly name for the null filling step.
        """
        if self.strategy == 'fill_mean':
            return f"Fill Nulls in '{self.column}' with Mean"
        elif self.strategy == 'fill_median':
            return f"Fill Nulls in '{self.column}' with Median"
        elif self.strategy == 'fill_mode':
            return f"Fill Nulls in '{self.column}' with Mode"
        elif self.strategy == 'fill_specific':
            return f"Fill Nulls in '{self.column}' with '{self.fill_value}'"
        return f"Unknown Fill Strategy: {self.strategy}"

    def get_config_for_yaml(self) -> dict:
        """
        Returns a dictionary representation of the cleaning step's configuration
        suitable for serialization into a YAML file for Fabric.
        """
        config = {
            "type": "fill_null_data",
            "column": self.column,
            "strategy": self.strategy
        }
        if self.fill_value is not None:
            config["fill_value"] = self.fill_value
        return config

    def get_parameters(self) -> dict:
        """
        Returns parameters for Fabric Notebook execution.
        These parameters would guide the notebook on how to perform the cleaning.
        """
        params = {
            "column": {"value": str(self.column), "type": "str"},
            "strategy": {"value": str(self.strategy), "type": "str"}
        }
        if self.fill_value is not None:
            params["fill_value"] = {"value": str(self.fill_value), "type": type(self.fill_value).__name__}
        return params

    def get_fabric_notebook_id(self) -> str:
        """
        Returns the ID of the Fabric Notebook responsible for this cleaning operation.
        """
        return "fabric_notebook_id_for_fill_nulls"

    def get_fabric_dataflow_activity_json(self) -> dict:
        """
        Returns the JSON structure for a Fabric Dataflow activity that performs
        this cleaning step.
        """
        return {
            "name": f"FillNulls_{self.column}_{self.strategy}",
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
        return bool(self.column and self.strategy)

