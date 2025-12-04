import re

EMAIL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

def filter_valid_emails(records):
    return [r for r in records if re.match(EMAIL_REGEX, r.get("email", ""))]