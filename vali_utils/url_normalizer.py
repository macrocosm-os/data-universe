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

    Key on IMMUTABLE content IDs only. The slug and subreddit (Reddit) and the
    username (X) are decorative and attacker-controlled: Reddit and the Apify
    actor resolve a post/comment by its base36 ID regardless of slug or even
    subreddit, and X resolves a tweet by ID regardless of username. Keying on
    those cosmetic fields let a miner mint unlimited "unique" URLs for one real
    piece of content (mutate the slug -> different hash -> counts as unique).

    X:      Only the numeric tweet ID identifies the content — username dropped.
    Reddit: Only post_id (+ comment_id) identify the content — sub + slug dropped.

    Canonical (ID-only) keys produced:
      X tweet:        x:{tweet_id}
      Reddit post:    reddit:{post_id}
      Reddit comment: reddit:{post_id}:{comment_id}
    """
    url = str(url_str).strip()
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    # Lowercase the path so an uppercased ID can't dodge the [a-z0-9] match
    # (which would truncate it and collapse distinct posts to a short key).
    path = parsed.path.lower()

    # --- X / Twitter ---
    if "x.com" in netloc or "twitter.com" in netloc:
        # Only the numeric tweet ID is identity; username is decorative and
        # attacker-controlled, so it is NOT part of the key.
        m = re.match(r"^/[^/]+/status/(\d+)", path)
        if m:
            return f"x:{m.group(1)}"
        return f"https://x.com{path.rstrip('/')}"

    # --- Reddit ---
    if "reddit.com" in netloc:
        # /r/{sub}/comments/{post_id}/{slug}/{comment_id}
        # Only post_id and comment_id are identity; sub and slug are
        # attacker-mutable (the actor echoes them back), so they are NOT keyed.
        m = re.match(
            r"^/r/[^/]+/comments/([a-z0-9]+)(?:/[^/]*(?:/([a-z0-9]+))?)?",
            path,
        )
        if m:
            post_id, comment_id = m.group(1), m.group(2)
            # Reddit IDs are lowercase base36 of VARIABLE length — they grow as
            # the ID space fills (6 chars historically, 7 since ~2023, 8 in
            # newer content; older comments can be shorter). So accept >=4 chars
            # rather than a fixed length (a fixed guard collapses every distinct
            # real comment whose id isn't that length into the bare post key).
            # A short junk segment (e.g. a trailing "f1" on some post URLs) is
            # under 4 chars, so it's ignored and the row keys as the post.
            if comment_id and re.match(r"^[a-z0-9]{4,}$", comment_id):
                return f"reddit:{post_id}:{comment_id}"
            return f"reddit:{post_id}"
        return f"https://www.reddit.com{path.rstrip('/')}"

    # --- Fallback: strip query/fragment, lowercase ---
    return f"{parsed.scheme}://{netloc}{path.rstrip('/')}"
