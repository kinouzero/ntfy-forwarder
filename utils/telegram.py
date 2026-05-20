def split_message(text, max_len):
    if text is None:
        return [""]

    text = str(text)
    if len(text) <= max_len:
        return [text]

    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break

        split_at = text.rfind("\n\n", 0, max_len)
        if split_at == -1:
            split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = text.rfind(" ", 0, max_len)
        if split_at == -1 or split_at < (max_len * 0.5):
            split_at = max_len

        parts.append(text[:split_at])
        text = text[split_at:].lstrip("\n ")

    return parts
