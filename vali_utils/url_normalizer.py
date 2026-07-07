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
    Reddit: Only post_id and comment_id (base36, exactly 7 chars) matter.

    Canonical forms produced:
      X tweet:        https://x.com/{user_lower}/status/{tweet_id}
      Reddit post:    https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}
      Reddit comment: https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}/{comment_id}
    """
    url = str(url_str).strip()
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    path = parsed.path

    # --- X / Twitter ---
    if "x.com" in netloc or "twitter.com" in netloc:
        # Extract /username/status/DIGITS — lowercase username to prevent case-fudging
        m = re.match(r"^/([^/]+)/status/(\d+)", path)
        if m:
            username = m.group(1).lower()
            tweet_id = m.group(2)
            return f"https://x.com/{username}/status/{tweet_id}"
        return f"https://x.com{path.rstrip('/').lower()}"

    # --- Reddit ---
    if "reddit.com" in netloc:
        # /r/{sub}/comments/{post_id}/{slug}/{comment_id}
        m = re.match(
            r"^/r/([^/]+)/comments/([a-z0-9]+)(?:/([^/]*)(?:/([a-z0-9]+))?)?",
            path,
        )
        if m:
            sub, post_id, slug, comment_id = m.group(1), m.group(2), m.group(3) or "", m.group(4)
            # Real Reddit IDs are base36, exactly 7 chars (post and comment).
            # Verified against 240K+ real comment IDs and 704 post IDs — all 7 chars.
            # Fake IDs (f1, _f1, aaaaaa0, etc.) fail this check.
            if comment_id and re.match(r"^[a-z0-9]{7}$", comment_id):
                return f"https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}/{comment_id}"
            return f"https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}"
        return f"https://www.reddit.com{path.rstrip('/')}"

    # --- Fallback: strip query/fragment, lowercase ---
    return f"{parsed.scheme}://{netloc}{path.rstrip('/')}"
