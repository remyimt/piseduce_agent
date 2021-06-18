from cryptography.fernet import Fernet
from lib.config_loader import get_config

# Remove unwanted charaters from the dangerous string
def safe_string(dangerous_str):
    return dangerous_str.translate({ord(c): "" for c in "\"!@#$%^&*()[]{};:,/<>?\|`~=+"})


def decrypt_password(pwd):
    with open(get_config()["key_file"], "r") as keyfile:
        key = keyfile.read()
        f = Fernet(key)
        return f.decrypt(pwd.encode()).decode()
    return ""
