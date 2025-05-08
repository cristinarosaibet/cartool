"""
Validate and transform data according to the metadata schema dfined in JSON file.
currently supports perfusion data

"""

import json
import pandas as pd
import numpy as np


def validate_and_transform_data(df_perfusion, data_folder):

    with open(os.path.join(data_folder, "metadata.json"), "r") as file:
        schema = json.load(file)

    # Validate the schema, check if the columns are in the schema
    pattern = re.compile(r"^(.+)_D-\d+$")

    for col in df_perfusion.columns:
        # check if column exists in the schema
        if col not in schema.keys():
            print(f"Column '{col}' is not in the schema.")
            continue
        else:
            match = pattern.match(col)
            # if the column is time-dependent
            if match:
                base_col_name = match.group(1)
                if base_col_name not in schema:
                    raise ValueError(f"Column '{base_col_name}' is not in the schema.")
            else:
                if col not in schema:
                    raise ValueError(f"Column '{col}' is not in the schema.")

    df_perfusion = cast_df_to_schema_types(df_perfusion, schema)

    return df_perfusion


def cast_df_to_schema_types(df, schema):
    for col, properties in schema.items():
        # Skip the columns that are not in the schema

        dtype = properties["type"]
        time_dependent = properties["time_dependent"]
        try:
            if time_dependent:
                cast_time_dependent_variable(df, col, dtype)
            else:
                if "int" in dtype.lower():
                    df[col] = pd.to_numeric(df[col], errors="raise").fillna(0).astype(dtype)
                elif "float" in dtype.lower():
                    df[col] = pd.to_numeric(df[col], errors="raise").astype(dtype)
                else:
                    df[col] = df[col].astype(dtype, errors="raise")
        except Exception as e:
            raise ValueError(f"Failed to convert column '{col}' to {dtype}: {e}")
    return df


def cast_time_dependent_variable(df, base_col_name, dtype):
    # Pattern to match columns like "VCD_D-10", "Viability_D-8", etc.
    pattern = re.compile(rf"^{re.escape(base_col_name)}_D-\d+$")

    for column in df.columns:
        if pattern.match(column):
            try:
                if "int" in dtype.lower():
                    df[column] = pd.to_numeric(df[column], errors="raise").fillna(0).astype(dtype)
                elif "float" in dtype.lower():
                    df[column] = pd.to_numeric(df[column], errors="raise").astype(dtype)
                else:
                    df[column] = df[column].astype(dtype)
            except Exception as e:
                raise ValueError(
                    f"Failed to convert time-dependent column '{column}' to {dtype}: {e}"
                )
