# It demonstrates how to remove null values from a DataFrame using PySpark.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json


def remove_nulls_template(
        spark: SparkSession,
        input_lakehouse_name: str,
        input_table_name: str,
        output_lakehouse_name: str,
        output_table_name: str,
        # Optional:
        columns_to_check: list = None,
        # Optional:
        how_to_drop: str = 'any'
):
    """
    Fabric Notebook template to remove null values from a specified Lakehouse table
    and write the cleaned data to another Lakehouse table.

    This function should be called within a Fabric Notebook context where `spark` is available.

    Args:
        spark (SparkSession): The active SparkSession instance provided by Fabric.
        input_lakehouse_name (str): The name of the input Lakehouse.
        input_table_name (str): The name of the input table within the Lakehouse.
        output_lakehouse_name (str): The name of the output Lakehouse.
        output_table_name (str): The name of the output table within the Lakehouse.
        columns_to_check (list, optional): A list of column names to check for nulls.
                                            If None, `how_to_drop` applies to all columns.
        how_to_drop (str, optional): 'any' or 'all'. Specifies if a row should be dropped
                                      if 'any' or 'all' of the specified columns have nulls.
                                      Defaults to 'any'.
    """
    print(f"Starting null removal process for table: {input_table_name} in Lakehouse: {input_lakehouse_name}")
    print(f"Output will be written to table: {output_table_name} in Lakehouse: {output_lakehouse_name}")
    print(f"Columns to check for nulls: {columns_to_check if columns_to_check else 'All Columns'}")
    print(f"How to drop rows: {how_to_drop}")

    try:

        df_input = spark.read.table(f"{input_lakehouse_name}.{input_table_name}")

        print(f"Successfully read data from {input_table_name}. Schema:")
        df_input.printSchema()
        print(f"Initial row count: {df_input.count()}")

        # Apply null removal logic
        if columns_to_check:
            # Drop rows where nulls exist in specified columns
            df_cleaned = df_input.dropna(how=how_to_drop, subset=columns_to_check)
        else:
            # Drop rows where nulls exist in any/all columns
            df_cleaned = df_input.dropna(how=how_to_drop)

        print(f"Cleaned row count: {df_cleaned.count()}")
        df_cleaned.write.format("delta").mode("overwrite").saveAsTable(f"{output_lakehouse_name}.{output_table_name}")

        print(f"Successfully wrote cleaned data to {output_table_name} in Lakehouse {output_lakehouse_name}.")

    except Exception as e:
        print(f"An error occurred during null removal: {e}")
        raise




if __name__ == "__main__":
    input_lh_name = globals().get("input_lakehouse_name", "mylakehouse")
    input_tbl_name = globals().get("input_table_name", "raw_data")
    output_lh_name = globals().get("output_lakehouse_name", "mylakehouse")
    output_tbl_name = globals().get("output_table_name", "cleaned_data_nulls_removed")

    cols_to_check_raw = globals().get("columns_to_check", None)
    if isinstance(cols_to_check_raw, str):
        try:
            cols_to_check = json.loads(cols_to_check_raw)
        except json.JSONDecodeError:
            print(f"Warning: columns_to_check received as invalid JSON string: {cols_to_check_raw}. Treating as None.")
            cols_to_check = None
    else:
        cols_to_check = cols_to_check_raw

    how_to_drop_option = globals().get("how_to_drop", "any")
    try:
        remove_nulls_template(
            spark,
            input_lakehouse_name=input_lh_name,
            input_table_name=input_tbl_name,
            output_lakehouse_name=output_lh_name,
            output_table_name=output_tbl_name,
            columns_to_check=cols_to_check,
            how_to_drop=how_to_drop_option
        )
    except NameError:
        print("\n--- SparkSession not found ---")
    except Exception as e:
        print(f"Script execution failed: {e}")

