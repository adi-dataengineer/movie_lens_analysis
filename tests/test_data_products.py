import os
import sys
import pytest
import pandas as pd
import yaml

try:
    from src.scripts.load_tools import gen_dataframe
    from src.app import movie_lens_analysis_pipeline
except ModuleNotFoundError:
    sys.path.append(".")
    from src.scripts.load_tools import gen_dataframe
    from src.app import movie_lens_analysis_pipeline


def test_movie_id_count_match():
    """Test if all movies in raw file are available in the final movies_with_ratings_stats dataset"""
    file_schema_path = "schema/movies.yml"
    file_path = "data/01_raw/movies.dat"
    df = gen_dataframe(file_path, file_schema_path)
    raw_movie_id_dis_cnt = df['movieid'].nunique()
    raw_movie_id_cnt = df['movieid'].count()

    file_schema_path = "schema/movies_with_ratings_stats.yml"
    file_path = "data/03_data_product/movies_with_ratings_stats.csv"
    df = gen_dataframe(file_path, file_schema_path)
    dp1_movie_id_dis_cnt = df['movieid'].nunique()
    dp1_movie_id_cnt = df['movieid'].count()

    assert raw_movie_id_dis_cnt == dp1_movie_id_dis_cnt
    assert raw_movie_id_cnt == dp1_movie_id_cnt


def test_movies_dp_schema():
    """Verify the schema of final movies_with_ratings_stats dataset"""
    file_schema_path = "schema/movies_with_ratings_stats.yml"
    with open(file_schema_path, "r") as file:
        file_schema = yaml.safe_load(file)

    column_names = [col["name"] for col in file_schema["columns"]]

    file_path = "data/03_data_product/movies_with_ratings_stats.csv"
    df = pd.read_csv(file_path)
    df_columns = df.columns.tolist()

    assert column_names == df_columns


def test_users_dp_schema():
    """Verify the schema of final top_movies_per_user dataset"""
    file_schema_path = "schema/top_movies_per_user.yml"
    with open(file_schema_path, "r") as file:
        file_schema = yaml.safe_load(file)

    column_names = [col["name"] for col in file_schema["columns"]]

    file_path = "data/03_data_product/top_movies_per_user.csv"
    df = pd.read_csv(file_path)
    df_columns = df.columns.tolist()

    assert column_names == df_columns


def test_user_id_count_match():
    """Test if all users in raw file are available in the final top_movies_per_user dataset"""
    file_schema_path = "schema/users.yml"
    file_path = "data/01_raw/users.dat"
    df = gen_dataframe(file_path, file_schema_path)
    raw_user_id_dis_cnt = df['userid'].nunique()

    file_schema_path = "schema/top_movies_per_user.yml"
    file_path = "data/03_data_product/top_movies_per_user.csv"
    df = gen_dataframe(file_path, file_schema_path)
    dp2_user_id_dis_cnt = df['userid'].nunique()

    assert raw_user_id_dis_cnt == dp2_user_id_dis_cnt


def test_user_id_groups():
    """Test if each user has 3 records and each user has unique movieid"""
    file_schema_path = "schema/top_movies_per_user.yml"
    file_path = "data/03_data_product/top_movies_per_user.csv"
    expected_id_count = 3

    df = gen_dataframe(file_path, file_schema_path)
    users_movie_counts = df.groupby('userid')['movieid'].nunique()
    all_users_movie_counts_match = (users_movie_counts == expected_id_count).all()

    dp2_user_ids = df['userid']
    userid_counts = dp2_user_ids.value_counts()
    all_user_ids_count_match = (userid_counts == expected_id_count).all()

    assert all_user_ids_count_match
    assert all_users_movie_counts_match