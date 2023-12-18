import yaml


class MovieDataValidator:
    """Data validation class to validate the movie dataset"""

    @staticmethod
    def validate_movieid(movieid):
        try:
            movieid_rng = int(movieid)
            return movieid_rng in range(1, 3953)
        except ValueError:
            return False


class UserDataValidator:
    """Data validation class to validate the user dataset"""

    @staticmethod
    def validate_gender(gender):
        return gender in ['M', 'F']

    @staticmethod
    def validate_age(age):
        try:
            age = int(age)
            return age in [1, 18, 25, 35, 45, 50, 56]
        except ValueError:
            return False

    @staticmethod
    def validate_occupation(occupation):
        try:
            occupation = int(occupation)
            return occupation in range(21)
        except ValueError:
            return False

    @staticmethod
    def validate_userid(userid):
        try:
            userid_rng = int(userid)
            return userid_rng in range(1, 6041)
        except ValueError:
            return False


class RatingDataValidator:
    """Data validation class to validate the ratings dataset"""

    @staticmethod
    def validate_ratings(ratings):
        try:
            ratings_rng = int(ratings)
            return ratings_rng in range(1, 6)
        except ValueError:
            return False

    @staticmethod
    def validate_movieid(movieid):
        result = MovieDataValidator.validate_movieid(movieid)
        return result

    @staticmethod
    def validate_userid(userid):
        result = UserDataValidator.validate_userid(userid)
        return result


def validate_dataset(validator_name, df, file_schema_path):
    """
    Validate columns of a DataFrame using a specific validator.

    Args:
    - validator_name (str): Name of the validator class.
    - df (pandas.DataFrame): DataFrame to be validated.
    - validation_columns (list): List of columns to be validated.

    Returns:
    - validation_results (dict): Dictionary containing validation results for each column.
    """
    validator_class = globals().get(validator_name, None)
    validation_results = {}

    with open(file_schema_path, 'r') as file:
        file_schema = yaml.safe_load(file)

    unique_check_columns = [col['name'] for col in file_schema['columns'] if
                            col['dqt_enabled'] and 'unique' in col['check_name']]

    if unique_check_columns:
        for column in unique_check_columns:
            column_series = df[column]
            unique_values = column_series.nunique()
            total_values = len(column_series)

            if unique_values == total_values:
                validation_result = True
            else:
                print(f"Data validation failed for column: {column}, in dataset: {validator_name}")
                validation_result = False

            validation_results[column] = validation_result

    if validator_class is not None:

        validation_columns = [col['name'] for col in file_schema['columns'] if
                              col['dqt_enabled'] and 'custom' in col['check_name']]

        if validation_columns:
            for column in validation_columns:
                dqt_func = getattr(validator_class, f'validate_{column}')
                invalid_df = df[~df[column].apply(dqt_func)]

                if not invalid_df.empty:
                    print(f"Data validation failed for column: {column}, in dataset: {validator_name}")
                    validation_result = False
                else:
                    validation_result = True

                validation_results[column] = validation_result

    return validation_results
