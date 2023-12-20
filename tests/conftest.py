import pytest
import os
import sys

try:
    from src.scripts.load_tools import gen_dataframe
    from src.app import movie_lens_analysis_pipeline
    from noxfile import delete_folders
except ModuleNotFoundError:
    sys.path.append(".")
    from src.scripts.load_tools import gen_dataframe
    from src.app import movie_lens_analysis_pipeline
    from noxfile import delete_folders


def cleanup():

    # delete_folder_paths_list = ['data/01_raw', 'data/02_curated', 'data/03_data_product']
    # delete_folders(folders_to_delete=delete_folder_paths_list, del_folders_flag=False)
    pass


@pytest.fixture(scope='session', autouse=True)
def cleanup_folders_after_tests(request):
    request.addfinalizer(cleanup)


@pytest.fixture(scope='session', autouse=True)
def run_movie_lens_analysis_pipeline():
    os.chdir('src')
    folder = 'data/03_data_product'

    try:
        files_in_folder = os.listdir(folder)
        if len(files_in_folder) != 2:
            print("DP Files missing, commencing movie_lens_analysis_pipeline")
            movie_lens_analysis_pipeline('config.ini')
    except FileNotFoundError:
        print("DP Folder missing, commencing movie_lens_analysis_pipeline")
        movie_lens_analysis_pipeline('config.ini')
