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
        "Static run": "Static_Run",
        "ambr15 run": "AMBR15_Run",
        "Conditions": "Conditions",
        "Agitation_Strategy": "Agitation_Strategy",
        "System": "System",
        "Agitation": "Agitation",
        "Activation reagent": "Activation_Reagent",
        "Activation time": "Activation_Time",
        "Cells/Microbeads": "Cells_per_Microbeads",
        "DO - activation": "DO_Activation",
        "DO - expansion": "DO_Expansion",
        "Cytokine supplementation": "Cytokine_Supplementation",
        "Inoculum (M cell/mL)": "Inoculum",
    }

    time_dependent_labels = {
        "Viable cell density (cell/mL)": "VCD",
        "Viability (%)": "Viability",
        "Lactate Concentration": "Lac",
        "Glucose Concentration": "Glc",
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

    # add colummn for the type of run
    run_index = df_perfusion.columns.get_loc("Run")
    df_perfusion.insert(run_index + 1, "Type", "Perfusion")
    # creat new columns
    df_perfusion = derive_new_columns(df_perfusion)

    return df_perfusion


def derive_new_columns(df_perfusion):
    # get the index of "System" column so that we can use it to create new columns
    system_index = df_perfusion.columns.get_loc("System")

    # Extract the working volume from AMBR15_run for STB systems
    df_perfusion.insert(system_index + 1, "Volume", None)
    # For rows where System is STB, extract digits before "mL" from AMBR15_run
    mask = df_perfusion["System"] == "STB"

    # Convert to string and extract the digits before "mL"
    df_perfusion.loc[mask, "Volume"] = (
        df_perfusion.loc[mask, "AMBR15_Run"].astype(str).str.extract(r"(\d+\.?\d*)").iloc[:, 0]
    )

    # Extract the volume from static run for Static systems
    map_nr_wells_volume = {"48": 0.4, "24": 0.1}

    mask = df_perfusion["System"] == "Static"

    # Convert to string and extract the digits before "mL"
    df_perfusion.loc[mask, "Nr_of_Wells"] = (
        df_perfusion.loc[mask, "Static_Run"]
        .astype(str)
        .str.extract(r"(\d+)(?=\s*-?\s*(?:well|wp))")
        .iloc[:, 0]
    )

    df_perfusion.loc[mask, "Volume"] = (
        df_perfusion.loc[mask, "Nr_of_Wells"].astype(str).map(map_nr_wells_volume)
    )

    df_perfusion.drop("Nr_of_Wells", axis=1, inplace=True)

    # Extract pH from the conditions and notes columns
    df_perfusion.insert(system_index + 2, "pH_Strategy", None)

    # default_value
    default_pH = 7.3
    df_perfusion["pH_Strategy"] = default_pH

    # Mask for "pH" in 'Conditions'
    mask_conditions = (
        df_perfusion["Conditions"]
        .astype(str)
        .str.contains(r"\bpH\b", case=False, na=False, regex=True)
    )
    df_perfusion.loc[mask_conditions, "pH_Strategy"] = df_perfusion.loc[
        mask_conditions, "Conditions"
    ]

    # Mask for "pH" in 'Notes'
    mask_notes = (
        df_perfusion["Notes"].astype(str).str.contains(r"\bpH\b", case=False, na=False, regex=True)
    )

    df_perfusion.loc[mask_notes, "pH_Strategy"] = df_perfusion.loc[mask_notes, "Notes"]

    # Count and preview
    num_ph_rows = df_perfusion["pH_Strategy"].dropna().ne(default_pH).sum()
    print(f"Found {num_ph_rows} rows with 'pH' mentioned needing manual review.")

    # Create feeding strategy column
    df_perfusion.insert(system_index + 3, "Feeding_Strategy", None)

    # Step 1: Split the range into two new columns
    df_perfusion[["start_date", "end_date"]] = df_perfusion["Date"].str.split(" to ", expand=True)

    # Step 2: Convert to datetime
    df_perfusion["end_date"] = pd.to_datetime(df_perfusion["end_date"])
    df_perfusion["start_date"] = pd.to_datetime(df_perfusion["start_date"])

    # Step 3: Filter rows where end_date is greater than or equal to 2023-09-01
    df_perfusion["Feeding_Strategy"] = np.where(
        df_perfusion["end_date"] >= pd.Timestamp("2023-09-01"), "A", None
    )
    print("Some rows of Feeding strategy column need to be added manually")
    df_perfusion.drop(columns=["start_date", "end_date"], inplace=True)

    return df_perfusion


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
