import configparser

import pandas as pd

from src.scripts.dqt import validate_dataset
from src.scripts.exceptions import CustomErrors
from src.scripts.load_tools import extract_and_load_dataset, gen_dataframe, prepare_file_zones


class DataProcessor:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        self.raw_zip_file_path = self.config["Paths"]["raw_zip_file_path"]
        self.raw_zone_path = self.config["Paths"]["raw_zone_path"]
        self.curated_zone_path = self.config["Paths"]["curated_zone_path"]
        self.data_product_zone_path = self.config["Paths"]["data_product_zone_path"]
        self.datasets_schema_path = self.config["Paths"]["datasets_schema_path"]
        self.source_datasets = self.config["Data"]["source_datasets"].split(", ")
        self.source_dqt_classes = self.config["Data"]["source_dqt_classes"].split(", ")
        self.source_datasets_mapper = dict(zip(self.source_dqt_classes, self.source_datasets))

        self.top_movies_per_user_dataset_config = self.config["top_movies_per_user_dataset_config"]
        self.movies_with_ratings_stats_dataset_config = self.config["movies_with_ratings_stats_dataset_config"]

    def prepare_infra(self):
        """Create the Directories to save the processed files"""
        prepare_file_zones([self.raw_zone_path, self.curated_zone_path, self.data_product_zone_path])
        return

    def staging_to_raw(self):
        """Extract dat files from zip folders"""
        for dataset in self.source_datasets:
            extract_and_load_dataset(self.raw_zip_file_path, self.raw_zone_path, dataset)
        print("Raw files loaded")

    def verify_dq_load_curated(self):
        """Verify Data quality and Load files to Curated zone"""
        datasets_mapper = self.source_datasets_mapper
        for validator_name in datasets_mapper:
            dataset = datasets_mapper[validator_name]
            validation_dataset_name = dataset.split(".")[0]
            file_schema_path = f"{self.datasets_schema_path}/{validation_dataset_name}.yml"
            raw_file_file_path = f"{self.raw_zone_path}/{dataset}"

            dataset_df = gen_dataframe(raw_file_file_path, file_schema_path)

            # Perform validation on the DataFrame
            self.verify_data_product_quality(file_name=validation_dataset_name, dqt_class=validator_name,
                                             dataset_df=dataset_df, file_schema_path=file_schema_path)

            print("Loading file to Curated Zone")
            self.df_to_csv(target_path=self.curated_zone_path, df=dataset_df, file_name=validation_dataset_name)

    def verify_dq_load_data_product1(self):
        # Step 1: Read curated movies.csv and ratings.csv into Pandas DataFrames
        movies_df = pd.read_csv(f"{self.curated_zone_path}/movies.csv")
        ratings_df = pd.read_csv(f"{self.curated_zone_path}/ratings.csv")

        # Step 2: Create a new DataFrame with max, min, and average ratings for each movie
        movie_ratings_stats = ratings_df.groupby("movieid")["ratings"].agg(["max", "min", "mean"]).reset_index()
        movie_ratings_stats.columns = ["movieid", "max_rating", "min_rating", "avg_rating"]
        movies_with_ratings_stats = pd.merge(movies_df, movie_ratings_stats, on="movieid", how="left")

        # Step 3: Perform Data Quality
        file_name = self.movies_with_ratings_stats_dataset_config['file_name']
        file_schema_path = f"{self.datasets_schema_path}/{self.movies_with_ratings_stats_dataset_config['schema_file_name']}"
        dqt_class = self.movies_with_ratings_stats_dataset_config['dqt_class']
        self.verify_data_product_quality(file_name=file_name, dqt_class=dqt_class, dataset_df=movies_with_ratings_stats, file_schema_path=file_schema_path)

        # Step 4: Load Curated
        print("Loading file to Data Product Zone")
        self.df_to_csv(target_path=self.data_product_zone_path, df=movies_with_ratings_stats, file_name=file_name)

    def verify_dq_load_data_product2(self):
        # Step 1: Read curated movies.csv and ratings.csv into Pandas DataFrames
        movies_df = pd.read_csv(f"{self.curated_zone_path}/movies.csv")
        ratings_df = pd.read_csv(f"{self.curated_zone_path}/ratings.csv")

        # Step 2: Create a new DataFrame containing each user's top 3 movies based on their ratings
        top_movies_per_user = ratings_df.sort_values(by=["userid"], ascending=[True]).groupby("userid").head(3)
        top_movies_per_user = pd.merge(top_movies_per_user, movies_df, on="movieid", how="left")

        # Step 3: Perform Data Quality
        file_name = self.top_movies_per_user_dataset_config['file_name']
        file_schema_path = f"{self.datasets_schema_path}/{self.top_movies_per_user_dataset_config['schema_file_name']}"
        dqt_class = self.top_movies_per_user_dataset_config['dqt_class']
        self.verify_data_product_quality(file_name=file_name, dqt_class=dqt_class, dataset_df=top_movies_per_user, file_schema_path=file_schema_path)

        # Step 4: Load Curated
        print("Loading file to Data Product Zone")
        self.df_to_csv(target_path=self.data_product_zone_path, df=top_movies_per_user, file_name=file_name)

    @staticmethod
    def verify_data_product_quality(file_name, dqt_class, dataset_df, file_schema_path):
        dqt_result = validate_dataset(validator_name=dqt_class, df=dataset_df, file_schema_path=file_schema_path)
        all_true_results = all(dqt_result.values())

        if all_true_results and dqt_result:
            print(f"Dataset: {file_name}, passed validation")
        elif not all_true_results and dqt_result:
            raise CustomErrors.DpDqtError(f"Dataset: {file_name}, failed validation, {dqt_result}")
        else:
            print(f"Data Validation for dataset: {file_name}, not enabled")

    @staticmethod
    def df_to_csv(target_path, df, file_name, float_format="%.2f", **kwargs):
        df.to_csv(f"{target_path}/{file_name}.csv", index=False, float_format=float_format, **kwargs)


def movie_lens_analysis_pipeline(config_path):
    processor = DataProcessor(config_path)

    # This process creates the basic folder structure for persisting the files
    processor.prepare_infra()

    # Extract .dat files from the zip file
    processor.staging_to_raw()

    # Verify data quality and Load to Curated layer
    processor.verify_dq_load_curated()

    # Verify data quality and Load to Data Product layer
    processor.verify_dq_load_data_product1()
    processor.verify_dq_load_data_product2()


if __name__ == "__main__":
    movie_lens_analysis_pipeline("config.ini")
