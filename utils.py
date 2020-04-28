from sqlalchemy import create_engine

from settings import RDBMS_TYPES


def check_input_data(data):
    """
    This function is used to check if input data are accepted or not

    Args:
        data: input data

    Returns: True or false

    """
    if 'uri' in data.keys() and 'type' in data.keys() and 'part' in data.keys():
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
