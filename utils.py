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
