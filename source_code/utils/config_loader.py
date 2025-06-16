import yaml

from source_code.utils.logger import get_logger

# Set up logging
logger = get_logger(__name__)


def load_yaml_config(path: str) -> dict:
    """
    Loads YAML configuration from the specified file path.

    Args:
        path (str): Path to the YAML config file.

    Returns:
        dict: Configuration as a dictionary.
    """
    try:
        logger.info("Loading configuration file from: %s", path)

        with open(path, "r") as f:
            config = yaml.safe_load(f)

        logger.info("Configuration loaded successfully.")
        return config

    except FileNotFoundError:
        logger.error("Configuration file not found at path: %s", path)
        raise  # Reraise the exception

    except yaml.YAMLError as e:
        logger.error("Error parsing YAML file: %s", str(e))
        raise  # Reraise the exception

    except Exception as e:
        logger.error(
            "An unexpected error occurred while loading the config file: %s", str(e)
        )
        raise  # Reraise the exception
