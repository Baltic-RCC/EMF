import configparser
import logging
import os

logger = logging.getLogger(__name__)


def parse_app_properties(caller_globals, path, section="MAIN", sanitize_mask="****"):
    # Configure settings parser
    raw_settings = configparser.RawConfigParser()
    raw_settings.optionxform = str

    # Load settings
    settings_path = os.path.join(path, "application.properties")
    raw_settings.read(settings_path)

    for setting in raw_settings.items(section):

        # Get parameter name and value from settings
        parameter_name, parameter_config_value = setting

        # Force parameter name to upper, to follow python PEP and also force that all settings are defined with cappital letters
        parameter_name = parameter_name.upper()

        # Get parameter value from ENV, if available
        parameter_env_value = os.getenv(parameter_name)

        # Check if password needs to be sanitized
        # TODO - maybe add list of keywords to function call and then here list comprehension and any()
        sanitize = "PASSWORD" in parameter_name

        # If parameter is defined in ENV
        if parameter_env_value:
            defined_in = "ENVIRONMENT"
            parameter_value = parameter_env_value

        # If not, take the default value form config
        else:
            defined_in = "PROPERTIES"
            parameter_value = parameter_config_value

        # Sanitize parameter value for logging
        sanitized_parameter_value = sanitize_mask if sanitize else parameter_value

        logger.info(f"{parameter_name} = {sanitized_parameter_value} [{defined_in}]",
                    extra={"parameter_defined_in": defined_in,
                           "parameter_name": parameter_name,
                           "parameter_value": sanitized_parameter_value})

        # Assign value to globals with upper letters
        caller_globals[parameter_name] = parameter_value

# TEST
if __name__ == "__main__":

    import sys
    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.DEBUG,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    os.environ["OPDM_PASSWORD"] = "1"

    parse_app_properties(globals(), path="../../config/opdm_integration")






