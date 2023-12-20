import os
import zipfile

import pandas as pd
import yaml

from src.scripts.exceptions import CustomErrors


def prepare_file_zones(directory_paths):
    """Create directories if they don't exist"""
    for directory in directory_paths:
        os.makedirs(directory, exist_ok=True)


def extract_and_load_dataset(zip_file_path: str, output_folder: str, dataset_name: str) -> None:
    """
    Extracts the specified dataset from a zip file and persists the file.

    Args:
    - zip_file_path (str): Path to the zip file containing the dataset.
    - dataset_name (str): Name of the dataset file to extract.

    Returns:
    - None

    Raises:
    - FileNotFoundError: If the specified dataset file is not found in the zip archive.
    - CustomErrors.MultiFileError: If multiple matching files are found.
    - Any other exceptions raised during extraction or loading.
    """
    try:
        with zipfile.ZipFile(zip_file_path, "r") as z:
            matching_files = [file for file in z.namelist() if dataset_name in file]

            if matching_files:
                if len(matching_files) > 1:
                    # If there are multiple matches, raise exception
                    raise CustomErrors.MultiFileError(f"Multiple files found for {dataset_name}")

                dataset_file = matching_files[0]

                with z.open(dataset_file) as file:
                    extracted_data = file.read()
                    output_path = f"{output_folder}/{dataset_name}"

                    with open(output_path, "wb") as output_file:
                        output_file.write(extracted_data)

            else:
                raise FileNotFoundError(f"No file matching '{dataset_name}' found in the zip archive.")
    except Exception as e:
        raise e

    return None


def gen_dataframe(dataset_file_path, file_schema_path):
    """
    Generates a Pandas DataFrame based on a given schema and dataset file.

    Args:
    - dataset_file_path (str): Path to the dataset file.
    - file_schema_path (str): Path to the file containing the schema information.

    Returns:
    - dataset_df (pandas.DataFrame): The generated DataFrame.
    """

    with open(file_schema_path, "r") as file:
        file_schema = yaml.safe_load(file)

    column_names = [col["name"] for col in file_schema["columns"]]

    dataframe = pd.DataFrame(columns=column_names)

    # Convert columns to appropriate raw_data types as per the schema
    for col in file_schema["columns"]:
        col_name = col["name"]
        col_type = col["type"]
        dataframe[col_name] = dataframe[col_name].astype(col_type)

    if dataset_file_path.endswith(".dat"):
        sep = "::"
        header = None
    else:
        sep = ","
        header = 0

    if dataset_file_path.endswith(".dat") or dataset_file_path.endswith(".csv"):
        dataset_df = pd.read_csv(dataset_file_path, sep=sep, engine="python", header=header, encoding="ISO-8859-1", names=column_names)
        return dataset_df
