import sys
import json
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:

    table_path = f"Tables/{table_name}"
    df_spark = spark.read.format("delta").load(table_path)

    schema_list = [{"name": f.name, "type": str(f.dataType)} for f in df_spark.schema]
    schema_json_content = json.dumps(schema_list, indent=2)

    sample_df_pandas = df_spark.limit(5).toPandas()

    # account = spark.conf.get("spark.hadoop.fs.abfss.account")
    # base_path = f"abfss://selfservingpipeline@onelake.dfs.fabric.microsoft.com/selfserving.Lakehouse/Files/sample_data/"
    base_path = f"/lakehouse/{lakehouse_id}/{output_base_path}/"
    mssparkutils.fs.mkdirs(output_base_path)

    schema_file_path = f"file:/lakehouse/default/Files/temp_data/schema.json"
    sample_file_path = f"file:/lakehouse/default/Files/temp_data/sample.csv"

    mssparkutils.fs.put(schema_file_path, schema_json_content, overwrite=True)

    # # Save sample data CSV using pandas
    sample_df_pandas.to_csv("/tmp/sample.csv", index=False)
    with open("/tmp/sample.csv", "r") as f:
        mssparkutils.fs.put(sample_file_path, f.read(), overwrite=True)



except Exception as e:
    error_msg = f"Error in notebook: {str(e)}"
    print(error_msg, file=sys.stderr)
    raise
