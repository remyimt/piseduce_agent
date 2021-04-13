
# Remove unwanted charaters from the dangerous string
def safe_string(dangerous_str):
    return dangerous_str.translate({ord(c): "" for c in "\"!@#$%^&*()[]{};:,/<>?\|`~=+"})
