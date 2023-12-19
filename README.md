# Movie Lens Analysis

### Repo setup instructions
1. Ensure Python 3.8 or above is used (Developed using Python 3.8)
2. Clone into the repo and Enable virtual env
3. Install App requirements using command: python -m pip install -r requirements.txt
4. Execute the [movie_lens_analysis_pipeline](src%2Fapp.py) using the command: nox -s exec_pipeline
5. The above command will ensure to populate the transformed data into the relevant folders under src/data

### About the pipeline and tests
1. The [movie_lens_analysis_pipeline](src%2Fapp.py) is structured to perform all tasks in an idempotent manner
2. The data pipeline is python class ([DataProcessor](src%2Fapp.py)) which requires only the config file located at - [src/config.ini](src%2Fconfig.ini)
3. The movie_lens_analysis_pipeline performs the following tasks (each is a specific method of the class):
   * Prepare the data process layers - raw, curated and data product layer
   * Extract .dat files from zip in [src/data/00_staging](src%2Fdata%2F00_staging) and Load to [src/data/01_raw](src%2Fdata%2F01_raw)
   * Verify Data quality and generate curated .csv files with schema applied. Schema files available here - [src/schema](src%2Fschema) and curated files loaded into [src/data/02_curated](src%2Fdata%2F02_curated)
   * Verify data quality, Apply transformation and Load final transformed dataset as .csv to Data Product layer [src/data/03_data_product](src%2Fdata%2F03_data_product)
4. Additionally reusable functions and custom exceptions to handle errors are located at - [src/scripts](src%2Fscripts)
5. [Unit Tests](tests): Test the code functionality and Business Logic applied

### Repo Structure
```
movie_lens_analysis
├── src
│   ├── data
│   │   ├── 00_staging
│   │   │   └── ml-1m.zip
│   │   ├── 01_raw
│   │   ├── 02_curated
│   │   └── 03_data_product
│   ├── schema
│   │   ├── movies.yml
│   │   ├── movies_with_ratings_stats.yml
│   │   ├── ratings.yml
│   │   ├── top_movies_per_user.yml
│   │   └── users.yml
│   ├── scripts
│   │   ├── dqt.py
│   │   ├── exceptions.py
│   │   └── load_tools.py
│   ├── app.py
│   └── config.ini
├── tests
├── noxfile.py
├── README.md
└── requirements.txt
```

### Development and Test Tools used 
```
[x] WindowsOS : For Development
[x] Python 3.8 : For Development
[x] Nox : For local automation of tasks
[x] Pytest: For execution of unit tests
[x] Black: Static Code check/code formatter
[x] isort: Static Code check/code formatter
[x] mypy: Static Code check/code formatter
```
