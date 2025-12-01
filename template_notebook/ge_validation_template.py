table_name = "selfserving.user1"
notebook_params={"pipeline_name": "", "execution_method": "Fabric Notebook", "validation_config":
{"great_expectations_suite":
{"data_asset_type": "PandasDataset", "expectations":
[{"expectation_type": "ExpectColumnToExist", "kwargs":
{"column": "video_view6"}}, {"expectation_type": "ExpectColumnValuesToNotBeNull", "kwargs":
 {"column": "video_view6"}}]}}}
validation_rules_json =notebook_params

import pandas as pd
import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration
import json

import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint import Checkpoint



df_spark = spark.sql(f"SELECT * FROM {table_name}")




df = df_spark.toPandas()

# Step 3: Initialize Great Expectations in-memory context
context = gx.get_context()

# Step 4: Create batch from pandas DataFrame
batch = context.data_sources.pandas_default.read_dataframe(df)
suite = gx.ExpectationSuite(name="fabric_suite")

expectation_args_map = {
    "ExpectColumnValuesToNotBeNull": ["column"],
    "ExpectColumnValuesToBeOfType": ["column", "type"],
    "ExpectColumnToExist": ["column"],
}

# Step 6: Add expectations based on your validation_objects
for obj in validation_rules_json["validation_config"]['great_expectations_suite']['expectations']:
    expectation_name = obj['expectation_type']
    expectation_class = getattr(gxe, expectation_name)
    if expectation_name in expectation_args_map:
        for arg in expectation_args_map[expectation_name]:
            if arg == 'column':
                args = obj["kwargs"]
                print(args)
            elif arg == 'type':
                args[arg] = obj["expected_type"]
        suite.add_expectation(expectation_class(**args))
# print(suite)
validation_result = batch.validate(suite)

# checkpoint = Checkpoint(
#     name="fabric_checkpoint",
#     data_context=context,
#     validations=[
#         {
#             "batch": batch,
#             "expectation_suite": suite.name,
#         }
#     ],
# )

# result = checkpoint.run()
# print(result)
# Assume `validation_result` is your result dict (like the one above)

if validation_result["success"]:
    clean_df.to_csv("/lakehouse/default/Files/temp_data/cleandf.csv", index=False)
    print("All expectations passed. Clean data saved.")
else:
    all_unexpected_indices = set()

    for result in validation_result["results"]:
        if not result["success"]:
            partial_indices = result["result"].get("partial_unexpected_index_list", [])
            all_unexpected_indices.update(partial_indices)

    df = df.reset_index(drop=True)  # Ensure index matches validation
    error_df = df.loc[list(all_unexpected_indices)]
    clean_df = df.drop(index=all_unexpected_indices).reset_index(drop=True)

    # Save both
    clean_df.to_csv("/lakehouse/default/Files/temp_data/cleandf.csv", index=False)
    error_df.to_csv("/lakehouse/default/Files/temp_data/errordf.csv", index=False)

    print(f"{len(all_unexpected_indices)} unexpected rows found and saved to 'errordf.csv'.")
    print(f"Remaining clean data saved to 'cleandf.csv'.")

# print(result)
# for
# unexpected_indices = set(result["results"][0]["result"]["partial_unexpected_index_list"])
# df = df.reset_index(drop=True)
# error_df = df.loc[list(unexpected_indices)]
# clean_df = df.drop(index=unexpected_indices).reset_index(drop=True)

# print(f"{len(clean_df)},{len(error_df)}")
# clean_df.to_csv("/lakehouse/default/Files/temp_data/cleandf.csv", index=False)
# error_df.to_csv("/lakehouse/default/Files/temp_data/errordf.csv", index=False)