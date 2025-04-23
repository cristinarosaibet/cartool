import numpy as np
import pandas as pd
import re
import pandera as pa
from dateparser import parse
import sys
import os
from cartool.clean_perfusion_data import clean_perfusion_data

pd.set_option("display.max_rows", None)  # Display all rows


def load_and_clean_bioprocess_data(path, data_folder):

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


def main():
    """
    Main function to execute the load and clean data process.
    """
    path = "data/processed/Main_Results_CARTool_2025-04-15.xlsx"
    data_folder = "data/processed/"
    load_and_clean_bioprocess_data(path, data_folder)


if __name__ == "__main__":
    main()
