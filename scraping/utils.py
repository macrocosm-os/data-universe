import datetime as dt


def obfuscate_datetime_to_minute(datetime_to_obfuscate: dt.datetime) -> dt.datetime:
    """_summary_

    Args:
        datetime_to_obfuscate (dt.datetime): Datetime to generate an obfuscated version of.

    Returns:
        dt.datetime: obfuscated datetime.
    """
    return datetime_to_obfuscate.replace(second=0, microsecond=0)
