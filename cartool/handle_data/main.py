from load_and_format_raw_data import unmerge_and_fill_cells
from load_and_clean_bioprocess_data import load_and_clean_bioprocess_data


def main():

    # load and format interim data to facilite data cleaning
    processed_data_folder = "data/processed/"

    interim_data = "data/interim/Main_Results_CARTool_2025-04-15.xlsx"
    processed_data = "data/processed/Main_Results_CARTool_2025-04-15.xlsx"
    unmerge_and_fill_cells(path_in=interim_data, path_out=processed_data)

    load_and_clean_bioprocess_data(processed_data, processed_data_folder)


if __name__ == "__main__":
    main()
