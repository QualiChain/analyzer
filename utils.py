import numpy as np
from sqlalchemy import create_engine

from settings import RDBMS_TYPES


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


def check_source(input_uri, type, part):
    """
    This function is used to check if provided source is valid
    Args:
        input_uri: provided uri
        type: RDBMS type or NoSQL
        part: table or collection

    Returns: True / False

    """

    if type in RDBMS_TYPES:
        _check = rdbms_check_if_uri_is_valid(input_uri, part)
    else:
        # TBD
        _check = False
    return _check


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
