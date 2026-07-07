"""Platform-aware URL normalization for dedup.

Lives in its own dependency-light module (stdlib only) so the dedup worker
processes can import it without pulling in bittensor/pandas/the full
s3_utils stack. s3_utils re-exports it, so existing imports keep working.
"""
import re
from urllib.parse import urlparse


def normalize_url_for_dedup(url_str: str) -> str:
    """Platform-aware URL normalization that extracts the canonical content ID for dedup.

    Approach: define what a VALID canonical URL looks like, ignore everything else.
    This is not a blacklist of known exploits — it's a whitelist of valid URL structure.

    X:      Only the numeric tweet ID matters. Username is lowercased (decorative — X resolves by ID).
    Reddit: Only post_id and comment_id (base36) matter. Subreddit and slug are decorative.

    Canonical forms produced:
      X tweet:        https://x.com/{user_lower}/status/{tweet_id}
      Reddit post:    reddit:{post_id}
      Reddit comment: reddit:{post_id}:{comment_id}
    """
    url = str(url_str).strip()
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    path = parsed.path.lower()

    # --- X / Twitter ---
    if "x.com" in netloc or "twitter.com" in netloc:
        m = re.match(r"^/([^/]+)/status/(\d+)", path)
        if m:
            return f"https://x.com/{m.group(1)}/status/{m.group(2)}"
        return f"https://x.com{path.rstrip('/')}"

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
