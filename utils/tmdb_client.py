import os
import configparser

def get_config(config_path='config/tmdb.conf'):
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), '..', config_path))
    return config