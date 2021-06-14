import json, os


CONFIG_SINGLETON = None


def load_config(config_path):
    global CONFIG_SINGLETON
    if CONFIG_SINGLETON is not None:
        return CONFIG_SINGLETON
    if os.path.exists(config_path):
        # Load the properties from the configuration file
        with open(config_path, 'r') as config_file:
            CONFIG_SINGLETON = json.load(config_file)
        # TODO Load the 'environment' property from the api module
        return CONFIG_SINGLETON
    raise LookupError("The configuration file '%s' is missing" % config_path)


def get_config():
    return CONFIG_SINGLETON
