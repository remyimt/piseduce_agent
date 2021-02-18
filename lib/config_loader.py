import json, os


CONFIG_PATH = "config_worker.json"
CONFIG_SINGLETON = None
DATE_FORMAT= "%Y-%m-%d %H:%M:%S"


def load_config():
    global CONFIG_SINGLETON
    if CONFIG_SINGLETON is not None:
        return CONFIG_SINGLETON
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as config_file:
            CONFIG_SINGLETON = json.load(config_file)
        return CONFIG_SINGLETON
    raise LookupError("The configuration file '%s' is missing" % CONFIG_PATH)
