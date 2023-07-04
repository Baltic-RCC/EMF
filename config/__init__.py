from pathlib import Path
from attrdict import AttrDict
import logging

logger = logging.getLogger(__name__)

# TEST
if __name__ == "__main__":
    import sys
    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.DEBUG,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# Get the directory path of the configuration files
config_directory = Path(__file__).resolve().parent

# List to store all configuration file paths
paths = AttrDict()
dirs_to_check = [config_directory]

# Recursively search for files in all folders
for path in dirs_to_check:
    for child_path in path.iterdir():

        # If path analyse further
        if child_path.is_dir():
            dirs_to_check.append(child_path)

        if "__" in child_path.stem:
            continue

        # Add the full path of the configuration file
        if child_path.is_file():
            logger.debug(f"Found config file {child_path.resolve()}")

            if not paths.get(child_path.parent.name):
                paths[child_path.parent.name] = AttrDict()

            paths[child_path.parent.name][child_path.stem] = child_path.resolve()


