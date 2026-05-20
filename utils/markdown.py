def escape_md(text):

    if text is None:
        return ""

    text = str(text)

    chars = r'\_*[]()~`>#+-=|{}.!'

    for c in chars:
        text = text.replace(
            c,
            f"\\{c}",
        )

    return text
