import configparser
import logging
import os
import inspect

logger = logging.getLogger(__name__)

def parse_app_properties(caller_globals):

    caller_path = os.path.abspath((inspect.stack()[1])[1])
    directory_of_caller = os.path.dirname(caller_path)

    settings = os.path.join(directory_of_caller, "application.properties")
    section = "MAIN"

    ## Parse application.properties configuration file
    raw_settings = configparser.RawConfigParser()
    raw_settings.optionxform = str
    raw_settings.read(settings)

    for setting in raw_settings.items(section):

        parameter_name, parameter_default_value = setting

        # First, lets see if parameter is defined in ENV
        parameter_value = os.getenv(parameter_name)

        # Check if parameter is defined in ENV
        if parameter_value:
            # If parameter values has commas - split into list
            if ',' in parameter_value:
                parameter_value = parameter_value.split(',')
            caller_globals[parameter_name] = parameter_value
            if "password" not in parameter_name.lower():
                logger.info(f"Parameter {parameter_name} defined in ENV -> {parameter_value}")
            else:
                logger.info(f"Parameter {parameter_name} defined in ENV -> ****")
        else:
            # If parameter values has commas - split into list
            if ',' in parameter_default_value:
                parameter_default_value = parameter_default_value.split(',')
            caller_globals[parameter_name] = parameter_default_value
            if "password" not in parameter_name.lower():
                logger.info(f"Parameter {parameter_name} not defined in ENV using default value from application.properties -> {parameter_default_value}")
            else:
                logger.info(f"Parameter {parameter_name} not defined in ENV using default value from application.properties -> ****")

    return caller_globals
