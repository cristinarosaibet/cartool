import numpy as np
import pandas as pd
import re
from dateparser import parse
import sys


def clean_perfusion_data(df_perfusion):
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

    validate_data(df_perfusion, time_dependent_labels)

    return df_perfusion


def validate_data(df_perfusion, time_dependent_labels):
    """
    Validate the perfusion data using pandera.
    """

    # schema of the non-time dependent labels
    schema = {
        "Date": "string",
        "Run": "Int64",
        "Donor": "Int64",
        "Static_run": "string",
        "AMBR15_run": "string",
        "Conditions": "string",
        "Agitation_Strategy": "string",
        "System": "string",
        "Agitation": "string",
        "Activation_reagent": "string",
        "Activation_time": "string",
        "Cells_per_Microbeads": "float",
        "DO_activation": "float",
        "DO_expansion": "float",
        "Cytokine_supplementation": "string",
        "Inoculum": "float",
        "Type": "string",
    }
    df_perfusion = cast_df_to_schema_types(df_perfusion, schema)

    # validate the time-dependent labels
    valid_prefixes = set(time_dependent_labels.values())

    # Pattern to match column names like 'VCD_D-10'
    pattern = re.compile(rf"^({'|'.join(valid_prefixes)})_D-\d+$")

    # Store invalid entries (optional)
    invalid_entries = []
    # Check each column that matches the pattern
    for col in df_perfusion.columns:
        if pattern.match(col):
            # Try converting to numeric
            coerced = pd.to_numeric(df_perfusion[col], errors="coerce")

            # Find non-numeric values that were not originally NaN
            non_numeric_mask = ~df_perfusion[col].isna() & coerced.isna()

            if non_numeric_mask.any():
                invalid_rows = df_perfusion[non_numeric_mask]
                invalid_entries.append((col, invalid_rows.index.tolist()))
                print(
                    f"⚠️ Non-numeric values found in column '{col}' at rows: {invalid_rows.index.tolist()}"
                )


def cast_df_to_schema_types(df, schema):
    missing_cols = set(schema.keys()) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in DataFrame: {missing_cols}")

    for col, dtype in schema.items():
        try:
            if "int" in dtype.lower():
                df[col] = pd.to_numeric(df[col], errors="raise").fillna(0).astype(dtype)
            if "float" in dtype:
                df[col] = pd.to_numeric(df[col], errors="raise").astype(dtype)
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            raise ValueError(f"Failed to convert column '{col}' to {dtype}: {e}")

    return df


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
    df["Run"] = df["cleaned"].str.extract(r"RUN\s*(\d+)", flags=re.IGNORECASE)[0]
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

    # Move "Run" to right after "Date"
    cols = df.columns.tolist()
    if "Run" in cols:
        cols.remove("Run")
        insert_at = cols.index("Date") + 1
        cols.insert(insert_at, "Run")
        df = df[cols]

    df1 = create_non_available_run_tag(df)

    return df


def create_non_available_run_tag(df_perfusion):

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
