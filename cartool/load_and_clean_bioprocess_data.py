import numpy as np
import pandas as pd
import re
import pandera as pa
from dateparser import parse
import sys


def load_and_clean_bioprocess_data(path):

    # Load the data
    with open(path, "rb") as file:
        df_perfusion = pd.read_excel(
            file,
            sheet_name="Main Results - Perfusion Mimick",
            header=None,  # No header in the perfusion data
        )

        df_fed_batch = pd.read_excel(file, sheet_name="Main Results - Fed Batch", header=None)

    # Clean the perfusion data
    df_perfusion_cleaned = clean_perfusion_data(df_perfusion)


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

    df_perfusion = merge_time_points_and_labels(time_dependent_labels, df_perfusion)
    print(df_perfusion.columns)
    df_perfusion = handle_date_column(df_perfusion)


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

    # rename colummns based on dictionary
    df.rename(columns=time_dependent_labels, inplace=True)

    # merge time points and labels
    subset = df[time_dependent_labels.values()]
    time_points = subset.iloc[0].to_numpy()
    labels = subset.columns

    if len(labels) == len(time_points):
        new_columns = [f"{label}_D-{int(tp)}" for label, tp in zip(labels, time_points)]
    else:
        raise ValueError(
            f"Number of labels ({len(labels)}) does not match number of time points ({len(time_points)})"
        )

    # apply new names to the selected columns
    rename_mapping = dict(zip(labels, new_columns))
    df.rename(columns=rename_mapping, inplace=True)

    # drop the first row (which had the time point values)
    df.drop(index=df.index[0], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


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
        match = re.search(r"(\d{1,2})\s*[-–]\s*(\d{1,2})\s*(\w+)", text)
        if match:
            d1, d2, month = match.groups()
            start = parse(f"{d1} {month} 2024")
            end = parse(f"{d2} {month} 2024")
        else:
            # Case 2: different months
            match = re.search(r"(\d{1,2})\s*(\w+)\s*[-–]\s*(\d{1,2})\s*(\w+)", text)
            if match:
                d1, m1, d2, m2 = match.groups()
                start = parse(f"{d1} {m1} 2024")
                end = parse(f"{d2} {m2} 2024")
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

    return df


def main():
    """
    Main function to execute the load and clean data process.
    """
    path = "data/processed/Main_Results_CARTool_2025-04-15.xlsx"
    load_and_clean_bioprocess_data(path)


if __name__ == "__main__":
    main()
