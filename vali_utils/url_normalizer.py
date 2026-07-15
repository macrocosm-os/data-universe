"""Platform-aware URL normalization for dedup.

Lives in its own dependency-light module (stdlib only) so the dedup worker
processes can import it without pulling in bittensor/pandas/the full
s3_utils stack. s3_utils re-exports it, so existing imports keep working.
"""
import re
import unicodedata
from urllib.parse import urlparse, unquote

# Strict tweet-URL shapes: /{user}/status/{id}, /i/web/status/{id},
# /statuses/{id}, optional /photo|video/N suffix. Anchored so a crafted
# path like /status/1/status/{id} cannot hijack or vary the captured ID.
_X_STATUS_RE = re.compile(
    r"^/(?:i/web/|[^/]+/)?status(?:es)?/(\d+)(?:/(?:photo|video)/\d+)?/?$"
)


def _decode_path(path: str) -> str:
    """Percent-decode a path to a fixpoint (bounded) so encoded variants of
    the same URL cannot mint distinct dedup keys. Decoding happens AFTER
    urlparse so a decoded '#' or '?' never gains structural meaning."""
    for _ in range(3):
        decoded = unquote(path)
        if decoded == path:
            break
        path = decoded
    return path


def normalize_url_for_dedup(url_str: str) -> str:
    """Platform-aware URL normalization that extracts the canonical content ID for dedup.

    Approach: define what a VALID canonical URL looks like, ignore everything else.
    This is not a blacklist of known exploits — it's a whitelist of valid URL structure.

    X:      Only the numeric tweet ID matters. The username segment is dropped entirely —
            X resolves any tweet by ID alone, so username variants of the same tweet
            must collapse to one key. Any x.com/twitter.com URL that is not a
            well-formed tweet URL collapses to a single sentinel key: crafted
            variants must not be able to mint distinct keys.
    Reddit: Only post_id and comment_id (base36) matter. Subreddit and slug are decorative.

    Canonical forms produced:
      X tweet:        x:{tweet_id}
      X non-tweet:    x:unparseable
      Reddit post:    reddit:{post_id}
      Reddit comment: reddit:{post_id}:{comment_id}
    """
    url = unicodedata.normalize("NFKC", str(url_str).strip())
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    if netloc.endswith(":443") or netloc.endswith(":80"):
        netloc = netloc.rsplit(":", 1)[0]
    netloc = netloc.rstrip(".")
    path = _decode_path(parsed.path.lower())

    # --- X / Twitter ---
    if "x.com" in netloc or "twitter.com" in netloc:
        m = _X_STATUS_RE.match(path)
        if m:
            # int() strips leading zeros so 0123 and 123 collapse.
            return f"x:{int(m.group(1))}"
        return "x:unparseable"

    # --- Reddit ---
    if "reddit.com" in netloc:
        # /r/{sub}/comments/... and /user/{name}/comments/... (profile posts) —
        # key on the IDs only.
        m = re.match(
            r"^/(?:r|user)/[^/]+/comments/([a-z0-9]+)(?:/[^/]*(?:/([a-z0-9]+))?)?",
            path,
        )
        if m:
            post_id, comment_id = m.group(1), m.group(2)
            # Reddit base36 IDs are variable length, so accept >=4 chars.
            if comment_id and re.match(r"^[a-z0-9]{4,}$", comment_id):
                return f"reddit:{post_id}:{comment_id}"
            return f"reddit:{post_id}"
        return f"https://www.reddit.com{path.rstrip('/')}"

    # --- Fallback: strip query/fragment, lowercase ---
    return f"{parsed.scheme}://{netloc}{path.rstrip('/')}"
