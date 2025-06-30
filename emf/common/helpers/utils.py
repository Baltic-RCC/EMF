import logging
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
from lxml import etree

logger = logging.getLogger(__name__)


def attr_to_dict(instance: object, sanitize_to_strings: bool = False):
    """
    Method to return class variables/attributes as dictionary
    pypowsybl._pypowsybl.LimitViolation -> dict
    :param instance: class instance
    :param sanitize_to_strings: flag to convert attributes to string type
    :return: dict
    """

    def convert_value(value):
        if isinstance(value, datetime):
            return value.isoformat() if sanitize_to_strings else value
        elif isinstance(value, list):
            return [convert_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: convert_value(v) for k, v in value.items()}
        elif hasattr(value, '__dict__'):
            return attr_to_dict(value, sanitize_to_strings)
        elif sanitize_to_strings:
            return str(value)
        return value

    attribs = [attr for attr in dir(instance) if (not callable(getattr(instance, attr)) and not attr.startswith("_"))]
    result_dict = {attr_key: convert_value(getattr(instance, attr_key)) for attr_key in attribs}

    return result_dict


def flatten_dict(nested_dict: dict, parent_key: str = '', separator: str = '.'):
    """
    Flattens a nested dictionary.

    Parameters:
    - nested_dict (dict): The dictionary to flatten.
    - parent_key (str): The base key string used for recursion.
    - separator (str): The separator between parent and child keys.

    Returns:
    - dict: A flattened dictionary where nested keys are concatenated into a single string.
    """
    items = []
    for k, v in nested_dict.items():
        new_key = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, separator=separator).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}[{i}]", separator=separator).items())
                else:
                    items.append((f"{new_key}[{i}]", item))
        else:
            items.append((new_key, v))
    return dict(items)


def convert_dict_str_to_bool(data_dict: dict):
    for key, value in data_dict.items():
        if isinstance(value, str):
            if value in ['True', 'true', 'TRUE']:
                data_dict[key] = True
            elif value in ['False', 'false', 'FALSE']:
                data_dict[key] = False
        elif isinstance(value, dict):
            # Recursively converter nested dictionaries
            data_dict[key] = convert_dict_str_to_bool(value)

    return data_dict


def filter_and_flatten_dict(nested_dict: dict, keys: list):
    """
    Creates a new flat dictionary from specified keys.

    Parameters:
    - nested_dict (dict): The original nested dictionary.
    - keys (list): The list of keys to include in the new flat dictionary.

    Returns:
    - dict: A new flat dictionary with only the specified keys.
    """
    flattened = flatten_dict(nested_dict)
    return {key: flattened[key] for key in keys if key in flattened}


def zip_xml(xml_file_object: BytesIO):
    xml_file_name = xml_file_object.name
    xml_file_extension = xml_file_name.split(".")[-1]
    zip_file_name = xml_file_name.replace(f".{xml_file_extension}", ".zip")

    zip_file_object = BytesIO()
    zip_file_object.name = zip_file_name

    # Create and save ZIP
    with ZipFile(zip_file_object, 'w', ZIP_DEFLATED) as zipfile:
        zipfile.writestr(xml_file_name, xml_file_object.getvalue())

    return zip_file_object


def get_xml_from_zip(zip_file_path: str):
    with ZipFile(zip_file_path, 'r') as zipfile_object:
        xml_file_name = zipfile_object.namelist()[0]
        file_bytes = zipfile_object.read(xml_file_name)
        # Convert bytes to file-like object for parsing
        xml_tree_object = etree.parse(BytesIO(file_bytes))

    return xml_tree_object


if __name__ == "__main__":
    pass