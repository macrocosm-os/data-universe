from urllib.parse import urlparse


def is_valid_reddit_url(url: str) -> bool:
    """Verifies a URL is both a valid URL and is for reddit.com."""
    if not url:
        return False

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and "reddit.com" in result.netloc
    except ValueError:
        return False
