def passes_filters(event_or_message):
    # Filtering via env vars has been removed.
    # Keep this helper as always-pass for compatibility with old imports/tests.
    return True
