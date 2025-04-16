import numpy as np
import pandas as pd


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

    format_time_dependent_labels(time_dependent_labels, df_perfusion)


def set_headers(time_dependent_labels, time_independent_labels, df_perfusion):
    """
    Set the headers for the perfusion data.
    """
    # Set the first row as the header
    df_perfusion.columns = df_perfusion.iloc[0]
    df_perfusion.drop(index=0, inplace=True)
    # df_perfusion.reset_index(drop=True, inplace=True)

    # Set columns name for labels more programmitcally friendly
    df_perfusion.rename(columns=time_independent_labels, inplace=True)
    df_perfusion.rename(columns=time_dependent_labels, inplace=True)

    return df_perfusion


def format_time_dependent_labels(time_dependent_labels, df_perfusion):
    # select subset of time dependent labels

    subset = df_perfusion[time_dependent_labels.values()].copy()
    # confirm if they are numeric
    subset.iloc[0] = subset.iloc[0].apply(lambda x: pd.to_numeric(x, errors="raise"))

    # Update the column names directly in the DataFrame
    new_columns = subset.columns + "_D-" + subset.iloc[0].astype(str)
    df_perfusion.rename(
        columns=dict(zip(time_dependent_labels.values(), new_columns)), inplace=True
    )
    df_perfusion.drop(index=1, inplace=True)
    df_perfusion.reset_index(drop=True, inplace=True)

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


def main():
    """
    Main function to execute the load and clean data process.
    """
    path = "data/processed/Main_Results_CARTool_2025-04-15.xlsx"
    load_and_clean_bioprocess_data(path)


if __name__ == "__main__":
    main()
