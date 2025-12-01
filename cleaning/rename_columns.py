import pandas as pd
from cleaning.base_cleaning import BaseCleaning
import re
import json  # For JSON serializing parameters to Fabric


class RenameColumns(BaseCleaning):
    """
    A cleaning operation to rename columns or apply naming conventions (lowercase, snake_case etc.).
    """

    def __init__(self, app_instance, utils_instance, add_lineage_step_func,
                 rename_type: str,
                 # 'specific_rename', 'to_lowercase', 'to_uppercase', 'to_snake_case', 'to_camel_case', 'trim_spaces'
                 old_name: str = None,
                 new_name: str = None,
                 columns_to_transform: list = None  # For case/space transformations
                 ):
        super().__init__(app_instance, utils_instance, add_lineage_step_func) # Removed display_name here
        self.rename_type = rename_type
        self.old_name = old_name
        self.new_name = new_name
        self.columns_to_transform = columns_to_transform
        self.config = {}

        if self.rename_type == 'specific_rename':
            if not self.old_name or not self.new_name:
                raise ValueError("Both old_name and new_name must be provided for specific_rename.")
        elif self.rename_type in ['to_lowercase', 'to_uppercase', 'to_snake_case', 'to_camel_case', 'trim_spaces']:
            if not self.columns_to_transform:
                raise ValueError("columns_to_transform must be provided for case/space transformations.")
        else:
            raise ValueError(f"Unknown rename_type: {self.rename_type}")

    def _to_snake_case(self, name: str) -> str:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def _to_camel_case(self, name: str) -> str:
        s = self._to_snake_case(name).replace("_", " ").title().replace(" ", "")
        return s[0].lower() + s[1:] if s else s

    def apply_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the column renaming/transformation logic.
        """
        df_copy = df.copy()
        applied_changes = []

        if self.rename_type == 'specific_rename':
            if self.old_name in df_copy.columns:
                df_copy.rename(columns={self.old_name: self.new_name}, inplace=True)
                applied_changes.append(f"Renamed '{self.old_name}' to '{self.new_name}'")
                self.config = {"type": self.rename_type, "old_name": self.old_name, "new_name": self.new_name,
                               "changes": applied_changes}
            else:
                self.app_instance.st.warning(f"Column '{self.old_name}' not found for specific rename. Skipping.")
                self.config = {"status": "skipped",
                               "reason": f"Column '{self.old_name}' not found for specific rename."}

        elif self.rename_type in ['to_lowercase', 'to_uppercase', 'to_snake_case', 'to_camel_case', 'trim_spaces']:
            for col in self.columns_to_transform:
                if col in df_copy.columns:
                    original_name = col
                    new_col_name = original_name
                    if self.rename_type == 'to_lowercase':
                        new_col_name = col.lower()
                    elif self.rename_type == 'to_uppercase':
                        new_col_name = col.upper()
                    elif self.rename_type == 'to_snake_case':
                        new_col_name = self._to_snake_case(col)
                    elif self.rename_type == 'to_camel_case':
                        new_col_name = self._to_camel_case(col)
                    elif self.rename_type == 'trim_spaces':
                        new_col_name = col.strip()

                    if original_name != new_col_name:
                        df_copy.rename(columns={original_name: new_col_name}, inplace=True)
                        applied_changes.append(
                            f"Transformed '{original_name}' to '{new_col_name}' ({self.rename_type})")
                else:
                    self.app_instance.st.warning(f"Column '{col}' not found for transformation. Skipping.")
            self.config = {"type": self.rename_type, "columns_transformed": self.columns_to_transform,
                           "changes": applied_changes}

        return df_copy

    def get_display_name(self) -> str:
        """
        Returns a user-friendly name for the column renaming step.
        """
        if self.rename_type == 'specific_rename':
            return f"Rename Column: '{self.old_name}' to '{self.new_name}'"
        elif self.rename_type == 'to_lowercase':
            return f"Convert Columns to Lowercase: {', '.join(self.columns_to_transform)}"
        elif self.rename_type == 'to_uppercase':
            return f"Convert Columns to Uppercase: {', '.join(self.columns_to_transform)}"
        elif self.rename_type == 'to_snake_case':
            return f"Convert Columns to Snake Case: {', '.join(self.columns_to_transform)}"
        elif self.rename_type == 'to_camel_case':
            return f"Convert Columns to Camel Case: {', '.join(self.columns_to_transform)}"
        elif self.rename_type == 'trim_spaces':
            return f"Trim Spaces from Columns: {', '.join(self.columns_to_transform)}"
        return f"Unknown Rename Type: {self.rename_type}"

    def get_config_for_yaml(self) -> dict:
        """
        Returns a dictionary representation of the cleaning step's configuration
        suitable for serialization into a YAML file for Fabric.
        """
        config = {
            "type": "rename_columns",
            "rename_type": self.rename_type
        }
        if self.old_name:
            config["old_name"] = self.old_name
        if self.new_name:
            config["new_name"] = self.new_name
        if self.columns_to_transform:
            config["columns_to_transform"] = self.columns_to_transform
        return config

    # --- Fabric Integration Placeholders ---
    def get_parameters(self) -> dict:
        """
        Returns parameters for Fabric Notebook execution.
        """
        params = {
            "rename_type": {"value": str(self.rename_type), "type": "str"}
        }
        if self.old_name:
            params["old_name"] = {"value": str(self.old_name), "type": "str"}
        if self.new_name:
            params["new_name"] = {"value": str(self.new_name), "type": "str"}
        if self.columns_to_transform:
            params["columns_to_transform"] = {"value": json.dumps(self.columns_to_transform), "type": "str"}
        return params

    def get_fabric_notebook_id(self) -> str:
        """
        Returns the ID of the Fabric Notebook responsible for this cleaning operation.
        """
        return "fabric_notebook_id_for_rename_columns"

    def get_fabric_dataflow_activity_json(self) -> dict:
        """
        Returns the JSON structure for a Fabric Dataflow activity.
        """
        activity_name = f"RenameCols_{self.rename_type}"
        if self.old_name and self.new_name:
            activity_name = f"RenameCol_{self.old_name}_to_{self.new_name}"

        return {
            "name": activity_name,
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
        if self.rename_type == 'specific_rename':
            return bool(self.old_name and self.new_name)
        elif self.rename_type in ['to_lowercase', 'to_uppercase', 'to_snake_case', 'to_camel_case', 'trim_spaces']:
            return bool(self.columns_to_transform)
        return False
