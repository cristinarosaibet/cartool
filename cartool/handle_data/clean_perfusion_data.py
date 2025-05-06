import numpy as np
import pandas as pd
import re
from dateparser import parse
import sys
import os
import json


def clean_perfusion_data(df_perfusion, data_folder):
    """
    Clean the perfusion data by removing unnecessary columns and renaming others.
    """

    df_perfusion = clean_strings(df_perfusion)

    # Get data from the dataframe
    time_independent_labels = {
        "Date": "Date",
        "Donor": "Donor",
        "Static run": "Static_run",
        "ambr15 run": "AMBR15_run",
        "Conditions": "Conditions",
        "Agitation_Strategy": "Agitation_Strategy",
        "System": "System",
        "Agitation": "Agitation",
        "Activation reagent": "Activation_reagent",
        "Activation time": "Activation_time",
        "Cells/Microbeads": "Cells_per_Microbeads",
        "DO - activation": "DO_activation",
        "DO - expansion": "DO_expansion",
        "Cytokine supplementation": "Cytokine_supplementation",
        "Inoculum (M cell/mL)": "Inoculum",
    }

    time_dependent_labels = {
        "Viable cell density (cell/mL)": "VCD",
        "Viability (%)": "Viability",
        "Lactate Concentration": "Lac",
        "CD25+ %": "CD25",
        "CD69+ %": "CD69",
        "PD-1+ %": "PD-1",
        "TIM-3+ %": "TIM-3",
        "LAG-3+ %": "LAG-3",
        "Naïve/Memory Stem T-cells %": "Naive_Memory",
        "Central Memory T-cells %": "Central_Memory",
        "Effector Memory T cells %": "Effector_Memory",
        "Effector T cells %": "Effector",
        "IFN-y+ (%)": "IFN-y",
        "TNF-a+ (%)": "TNF-a",
        "IFN-y+ TNF-a+​ (%)": "IFN-y_TNF-a",
        "CD4:CD8 ratio": "CD4_CD8_ratio",
    }

    df_perfusion = set_headers(time_dependent_labels, time_independent_labels, df_perfusion)
    # merge time points and labels info into one column label
    df_perfusion = merge_time_points_and_labels(time_dependent_labels, df_perfusion)
    # split info of date column into run and donor
    df_perfusion = handle_date_column(df_perfusion)
    # add colummn for the type of info
    df_perfusion["Type"] = "Perfusion"

    df_perfusion = validate_and_transform_data(df_perfusion, data_folder)

    return df_perfusion


def validate_and_transform_data(df_perfusion, data_folder):
    """
    Validate the perfusion data using pandera.
    """
    with open(os.path.join(data_folder, "metadata.json"), "r") as file:
        schema = json.load(file)

    df_perfusion = cast_df_to_schema_types(df_perfusion, schema)

    # validate the time-dependent labels

    return df_perfusion


def cast_df_to_schema_types(df, schema):
    for col, properties in schema.items():
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
                    df[col] = df[col].astype(dtype)
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


def set_headers(time_dependent_labels, time_independent_labels, df_perfusion):
    """
    Set the headers for the perfusion data.
    """
    # Set the first row as the header
    df_perfusion.columns = df_perfusion.iloc[0]
    df_perfusion.drop(index=0, inplace=True)

    # Set columns name for labels more programmitcally friendly
    df_perfusion.rename(columns=time_independent_labels, inplace=True)
    df_perfusion.rename(columns=time_dependent_labels, inplace=True)

    return df_perfusion


def merge_time_points_and_labels(time_dependent_labels, df_perfusion):
    df = df_perfusion.copy()

    # Rename columns based on dictionary
    df.rename(columns=time_dependent_labels, inplace=True)

    # Get column positions for columns we want to modify
    target_values = list(time_dependent_labels.values())
    target_positions = []

    # Find all positions of our target columns (including duplicates)
    for i, col in enumerate(df.columns):
        if col in target_values:
            target_positions.append(i)

    # Get all current column names
    column_names = list(df.columns)

    # Modify column names at target positions
    for pos in target_positions:
        col_name = column_names[pos]
        # Get the value from the first row at this column position
        value = df.iloc[0, pos]
        # Create the new column name
        column_names[pos] = f"{col_name}_D-{value}"

    # Apply the new column names
    df.columns = column_names

    # Drop the first row and reset index
    df.drop(index=df.index[0], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def handle_date_column(df):
    df = df.copy()  # Avoid SettingWithCopyWarning

    # Clean HTML and whitespace
    df["cleaned"] = df["Date"].replace(r"<.*?>", "", regex=True).str.strip()

    # Extract Run info
    run_info = df["cleaned"].str.extract(r"RUN\s*(\d+)", flags=re.IGNORECASE)[0]
    df.insert(1, "Run", run_info)
    df["Run"] = df["Run"].astype("Int64")  # allows for NaN-friendly integers

    # Extract Donor info (overwrite only where new info is found)
    new_donor = df["cleaned"].str.extract(r"Donor\s*(\d+)", flags=re.IGNORECASE)[0]
    df["Donor"] = new_donor.combine_first(df["Donor"])

    # Function to extract and format date ranges
    def extract_dates_str(text):
        text = str(text)
        # Case 1: same month (add year)
        match = re.search(r"(\d{1,2})\s*[-–]\s*(\d{1,2})\s*(\w+)\s*(\d{4})", text)
        if match:
            d1, d2, month, year = match.groups()
            start = parse(f"{d1} {month} {year}")
            end = parse(f"{d2} {month} {year}")
        else:
            # Case 2: different months
            match = re.search(r"(\d{1,2})\s*(\w+)\s*[-–]\s*(\d{1,2})\s*(\w+)\s*(\d{4})", text)
            if match:
                d1, m1, d2, m2, year = match.groups()
                start = parse(f"{d1} {m1} {year}")
                end = parse(f"{d2} {m2} {year}")
            else:
                return None

        if start and end:
            return f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"
        return None

    # Apply to cleaned column
    df["Date"] = df["cleaned"].apply(extract_dates_str)
    df.drop(columns=["cleaned"], inplace=True)

    df = create_non_available_run_tag(df)

    return df


def create_non_available_run_tag(df_perfusion):
    # if there wasn't info about the run, we create a new one
    unique_dates = df_perfusion["Date"].unique()
    runs = df_perfusion["Run"].unique().to_numpy()

    for i, date in enumerate(unique_dates):
        if df_perfusion.loc[df_perfusion["Date"] == date, "Run"].isna().all():
            run_number = i + 1
            while True:
                if run_number in runs:
                    # print(run_number)
                    run_number += 1
                else:
                    runs = np.append(runs, run_number)
                    break

            df_perfusion.loc[df_perfusion["Date"] == date, "Run"] = run_number

    return df_perfusion


def clean_strings(df):
    # remove leading and trailing spaces from all cells
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # remove \n
    df = df.replace("\n", " ", regex=True)

    # Replace values that "like" (contain) the substring "Media" with NaN
    df = df.map(lambda x: np.nan if isinstance(x, str) and "Media" in x else x).infer_objects(
        copy=False
    )

    df = df.replace("Not acquired", np.nan).infer_objects(copy=False)
    df = df.replace("-", np.nan).infer_objects(copy=False)
    df = df.replace("not acq", np.nan)

    return df
