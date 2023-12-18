import pandas as pd
from src.scripts.load_tools import extract_and_load_dataset, gen_dataframe
from src.scripts.dqt import validate_dataset


def main():
    zip_file_path = 'data/ml-1m.zip'
    datasets = ['movies.dat', 'users.dat', 'ratings.dat']
    dqt = ['MovieDataValidator', 'UserDataValidator', 'RatingDataValidator']
    datasets_mapper = dict(zip(dqt, datasets))

    for dataset in datasets:
        extract_and_load_dataset(zip_file_path, dataset)

    for validator_name in datasets_mapper:
        dataset = datasets_mapper[validator_name]
        validation_dataset_name = dataset.split(".")[0]
        file_schema_path = f"schema/{validation_dataset_name}.yml"
        file_path = f"data/{dataset}"
        dataset_df = gen_dataframe(file_path, file_schema_path)

        # Perform validation on the DataFrame
        dqt_result = validate_dataset(validator_name, dataset_df, file_schema_path)
        all_true_results = all(dqt_result.values())

        if all_true_results and dqt_result:
            print(f"Dataset: {validation_dataset_name}, passed validation")
        elif not all_true_results and dqt_result:
            print(f"Dataset: {validation_dataset_name}, failed validation")
            print(dqt_result)
        else:
            print(f"Data Validation for dataset: {validation_dataset_name}, not enabled")

    # Step 1: Read movies.dat and ratings.dat into Pandas DataFrames
    file_schema_path = f"schema/movies.yml"
    file_path = f"data/movies.dat"
    movies_df = gen_dataframe(file_path, file_schema_path)
    file_schema_path = f"schema/ratings.yml"
    file_path = f"data/ratings.dat"
    ratings_df = gen_dataframe(file_path, file_schema_path)

    # Step 2: Create a new DataFrame with max, min, and average ratings for each movie
    movie_ratings_stats = ratings_df.groupby('movieid')['ratings'].agg(['max', 'min', 'mean']).reset_index()
    movie_ratings_stats.columns = ['movieid', 'max_rating', 'min_rating', 'avg_rating']
    movies_with_ratings_stats = pd.merge(movies_df, movie_ratings_stats, on='movieid', how='left')

    # Step 3: Create a new DataFrame containing each user's top 3 movies based on their ratings
    top_movies_per_user = ratings_df.sort_values(by='ratings', ascending=False).groupby('userid').head(3)

    # Show the results
    print("Movies with Ratings Stats:")
    print(movies_with_ratings_stats.head())

    print("\nTop 3 Movies per User:")
    print(top_movies_per_user.head())


if __name__ == '__main__':
    main()
