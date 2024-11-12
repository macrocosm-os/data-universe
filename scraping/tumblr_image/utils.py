# scraping/tumblr_image/utils.py

import io
import re
import aiohttp
import datetime as dt
import bittensor as bt
from PIL import Image
from typing import Tuple, Optional, Dict, Any
from urllib.parse import urlparse
import requests

async def download_image_url_to_bytes(url: str) -> Tuple[Optional[bytes], Optional[int]]:
    """
    Downloads an image from URL and returns both bytes and content length.

    Args:
        url (str): Image URL to download

    Returns:
        Tuple[Optional[bytes], Optional[int]]: Image bytes and content length
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    bt.logging.error(f"Failed to download {url}, status: {response.status}")
                    return None, None

                content = await response.read()
                if not is_valid_image(content):
                    return None, None

                return content, len(content)
    except Exception as e:
        bt.logging.error(f"Error downloading {url}: {str(e)}")
        return None, None


def is_valid_image(image_bytes: bytes) -> bool:
    """Validates if bytes contain a valid image"""
    try:
        img = Image.open(io.BytesIO(image_bytes))
        img.verify()
        return True
    except Exception as e:
        bt.logging.error(f"Invalid image data: {str(e)}")
        return False


def extract_main_image_url(post_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract only the main/first image URL from a Tumblr post.

    Args:
        post_data (Dict): Raw post data from Tumblr API

    Returns:
        Optional[str]: URL of the main image or None
    """
    # Primary method: Check 'photos' field
    if 'photos' in post_data and post_data['photos']:
        first_photo = post_data['photos'][0]
        if 'original_size' in first_photo and 'url' in first_photo['original_size']:
            return first_photo['original_size']['url']

    # Fallback method: Check post body HTML
    if 'body' in post_data:
        # Look for first image matching Tumblr's media domain
        match = re.search(r'src="(https://64\.media\.tumblr\.com/[^"]+)"', post_data['body'])
        if match:
            return match.group(1)

    return None


def convert_timestamp_to_utc(timestamp: float) -> dt.datetime:
    """Convert a UNIX timestamp to UTC datetime"""
    return dt.datetime.fromtimestamp(timestamp, dt.timezone.utc)


def parse_tumblr_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse a Tumblr URL to extract blog name and post ID"""
    if not url:
        return None, None

    try:
        # Standard Tumblr URL format
        match = re.match(r'https?://([^.]+)\.tumblr\.com/post/(\d+)', url)
        if match:
            return match.group(1), match.group(2)

        # Handle custom domains
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        if 'post' in path_parts:
            post_idx = path_parts.index('post')
            if len(path_parts) > post_idx + 1:
                return parsed.netloc, path_parts[post_idx + 1]
    except Exception as e:
        bt.logging.error(f"Error parsing Tumblr URL {url}: {str(e)}")

    return None, None


def is_valid_photo_post(post_data: Dict[str, Any]) -> bool:
    """
    Validate if post is a valid photo post with a main image.

    Args:
        post_data (Dict): Raw post data from Tumblr API

    Returns:
        bool: True if post has a valid main photo
    """
    # Must be photo type
    if post_data.get('type') != 'photo':
        return False

    # Must have at least one photo
    if not post_data.get('photos'):
        return False

    # Must have a valid original size URL for the first photo
    first_photo = post_data['photos'][0]
    if not (first_photo.get('original_size') and
            first_photo['original_size'].get('url')):
        return False

    return True


def get_post_caption(post_data: Dict[str, Any]) -> str:
    """
    Get the caption/description of the main photo.

    Args:
        post_data (Dict): Raw post data from Tumblr API

    Returns:
        str: Caption text or empty string
    """
    # Try to get caption from summary
    if post_data.get('summary'):
        return post_data['summary']

    # Try to get caption from first photo
    if post_data.get('photos') and post_data['photos'][0].get('caption'):
        return post_data['photos'][0]['caption']

    # Fallback to empty string
    return ""


# Example of how this would be used in the TumblrContent model:


""""
def _best_effort_parse_tumblr_image(self, tumblr_image_dataset: List[dict]):
    results = []
    for post in tumblr_image_dataset:
        # Check if it's a valid photo post
        if not is_valid_photo_post(post):
            continue

        # Get main image URL
        image_url = extract_main_image_url(post)
        if not image_url:
            continue

        # Download image
        image_bytes, image_content_size = await download_image_url_to_bytes(image_url)
        if not image_bytes:
            continue

        # Create content object
        content = TumblrContent(
            timestamp=convert_timestamp_to_utc(post['timestamp']),
            image_bytes=image_bytes,
            tags=post.get('tags', []),
            description=get_post_caption(post),
            post_url=post.get('post_url', ''),
            creator=post.get('blog_name', '')
        )
        results.append(content)

    return results
"""