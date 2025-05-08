"""
This script automates the initial preprocessing steps for bioprocess data:
1. Unmerges and fills Excel cells from raw interim files.
2. Loads and cleans 'Perfusion Mimick' and 'Fed Batch' sheets.
3. Outputs intermediate cleaned files for manual inspection.

After running this script:
   - Manually review and (if needed) edit the file perfusion_data.csv in `data/processed/`.
   - Then run the validation and transformation script.
"""

import os
import pandas as pd
from load_and_format_raw_data import unmerge_and_fill_cells
from clean_perfusion_data import clean_perfusion_data


def process_data(date="2025-04-15"):
    processed_data_folder = "data/processed/"
    interim_data = f"data/interim/Main_Results_CARTool_{date}.xlsx"
    processed_data = f"{processed_data_folder}Main_Results_CARTool_{date}.xlsx"

    unmerge_and_fill_cells(path_in=interim_data, path_out=processed_data)
    load_and_clean_bioprocess_data(processed_data, processed_data_folder)

    print(f"Data cleaned and saved to {processed_data_folder}. Please review before continuing.")


def load_and_clean_bioprocess_data(path, output_folder):
    with open(path, "rb") as file:
        df_perfusion = pd.read_excel(
            file, sheet_name="Main Results - Perfusion Mimick", header=None
        )
        df_fed_batch = pd.read_excel(file, sheet_name="Main Results - Fed Batch", header=None)

    df_perfusion_cleaned = clean_perfusion_data(df_perfusion, output_folder)
    output_path = os.path.join(output_folder, "perfusion_data.csv")

    with open(output_path, "w") as file:
        df_perfusion_cleaned.to_csv(file, index=False)


if __name__ == "__main__":
    process_data()
