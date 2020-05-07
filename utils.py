import re

import numpy as np
from sqlalchemy import create_engine

from clients.elastic_client import ElasticClient
from clients.rdbms_client import RDBMSClient


def check_input_data(data):
    """
    This function is used to check if input data are accepted or not

    Args:
        data: input data

    Returns: True or false

    """
    if 'uri' in data.keys() and 'type' in data.keys() and 'part' in data.keys() and 'index' in data.keys():
        return True
    else:
        return False


def rdbms_check_if_uri_is_valid(input_uri, part):
    """
    This function test if the provided connection URI is valid

    Args:
        input_uri: input uri
        part: provided table

    Returns: True/False

    """
    try:
        engine = create_engine(input_uri)

        table_names = engine.table_names()
        if part in table_names:
            return True
        else:
            return False
    except Exception as ex:
        return False


def map_dtype_to_elk_type(df_type):
    """
    This function is used to map data frame types to elastic search types
    Args:
        df_type: provided type

    Returns: elastic search data type

    """
    if df_type == np.int64:
        return_type = {'type': 'integer'}
    elif df_type == np.float64:
        return_type = {'type': 'float'}
    elif df_type == np.object:
        return_type = {'type': 'text'}
    elif df_type == np.bool:
        return_type = {'type': 'boolean'}
    elif df_type == np.datetime:
        return_type = {'type': 'date'}
    return return_type


def df_lookup(data_frame):
    """
    This function is used to find data frame types

    Args:
        data_frame: provided data frame

    Returns: processed data frame

    """
    data_frame_types = data_frame.dtypes
    type_items = data_frame_types.items()
    transformed_types = dict(map(lambda element: (element[0], map_dtype_to_elk_type(element[1])), type_items))
    return transformed_types


def replace_nan_in_files(data_frame):
    """
    This function is used to remove NaN in provided files

    Args:
        data_frame: data frame

    Returns: dataframe without NaN

    """
    df_without_nan = data_frame.replace(np.nan, '', regex=True)
    return df_without_nan


def split_camel_case(input_string):
    """
    This function is used to transform camel case words to more words

    Args:
        input_string: camel case string

    Returns: Extracted words from camel case

    """
    splitted = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', input_string)).split()
    joined_string = " ".join(splitted)
    return joined_string



def test_pipeline(file_df, index2use):
    ## for testing purposes , later in async way
    csv_without_nan = replace_nan_in_files(file_df)
    input_types = df_lookup(csv_without_nan)

    es = ElasticClient()
    es.create_index(index=index2use, properties=input_types)
    es.insert_source_data(csv_without_nan, index2use)
    ##


def test_rdbms_pipeline(input_uri, part, index2use):
    ## for testing purposes , later in async way

    rdbms = RDBMSClient(input_uri)
    table_df = rdbms.load_table(part)
    input_types = df_lookup(table_df)

    es = ElasticClient()
    es.create_index(index=index2use, properties=input_types)

    es.insert_source_data(table_df, index2use)
    ##


def test_mongo_pipeline(data_frame, index2use):
    ## for testing purposes , later in async way
    input_types = df_lookup(data_frame)

    es = ElasticClient()
    es.create_index(index=index2use, properties=input_types)

    es.insert_source_data(data_frame, index2use)
