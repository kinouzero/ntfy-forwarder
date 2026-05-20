import re

def passes_filters(message):

    excluded = [
        r"DEBUG",
    ]

    for pattern in excluded:
        if re.search(pattern, message, re.I):
            return False

    return True
