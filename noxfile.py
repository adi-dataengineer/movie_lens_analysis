import os
import sys
import configparser
import shutil
import nox

try:
    from src.app import movie_lens_analysis_pipeline
except ModuleNotFoundError:
    sys.path.append(".")
    from src.app import movie_lens_analysis_pipeline

nox.options.stop_on_first_error = True
python_versions = ["3.8"]


def delete_folders(folders_to_delete: list, del_folders_flag: bool = False) -> None:
    for folder in folders_to_delete:
        try:
            # List all files and folders within the directory
            files_in_folder = os.listdir(folder)

            # Delete each file in the folder
            for file_name in files_in_folder:
                file_path = os.path.join(folder, file_name)
                os.remove(file_path)
                print(f"Deleted file: {file_name} from folder {folder}")

            if del_folders_flag:
                # Delete the folder itself after deleting the files
                shutil.rmtree(folder)
                print(f"Deleted folder: {folder}")
        except FileNotFoundError:
            print(f"Folder not found: {folder}")


@nox.session(python=python_versions, reuse_venv=True, tags=['prep-env', 'exec-pipeline', 'lint-files'])
def clean_folders(session: nox.Session):
    config = configparser.ConfigParser()
    config.read('src/config.ini')
    raw_zone_path = config['Paths']['raw_zone_path']
    curated_zone_path = config['Paths']['curated_zone_path']
    data_product_zone_path = config['Paths']['data_product_zone_path']

    folder_paths = [raw_zone_path, curated_zone_path, data_product_zone_path]
    delete_folder_paths_list = [f"src/{x}" for x in folder_paths]

    delete_folders(folders_to_delete=delete_folder_paths_list, del_folders_flag=False)


@nox.session(python=python_versions, reuse_venv=False, tags=['exec-pipeline'])
def exec_pipeline(session: nox.Session):
    session.chdir('src')
    movie_lens_analysis_pipeline('config.ini')
    session.notify("exec_tests")


@nox.session(python=python_versions, reuse_venv=True, tags=['lint-files'])
def lint_alert(session: nox.Session, folder_name: str = 'src') -> None:
    """Lint files"""
    session.run("isort", folder_name, "--line-length=300", external=True)
    session.run("black", folder_name, "--line-length=300", external=True)
    # session.run("mypy", folder_name, external=True)


@nox.session(python=python_versions, reuse_venv=True, tags=['exec-tests'])
def exec_tests(session: nox.Session) -> None:
    """Lint files"""
    session.run("pytest", "-v", "-s", external=True)
