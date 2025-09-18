import datetime as dt
import random
from typing import Optional, List
import bittensor as bt
from urllib.parse import urlparse

from common.data import DataEntity
from scraping.scraper import ValidationResult
from scraping.web.model import WebContent


def is_valid_web_url(url: str) -> bool:
    """Check if a URL is valid for web scraping."""
    if not url:
        return False
    
    try:
        parsed = urlparse(url)
        return parsed.scheme in ('http', 'https') and bool(parsed.netloc)
    except Exception:
        return False


def validate_web_content_with_rescrape(
    miner_content: WebContent,
    validator_scraped_content: WebContent,
    entity: DataEntity,
) -> ValidationResult:
    """
    Validate web content by comparing miner submission with validator re-scraped content.
    Similar to SN22 validation approach.
    """
    
    # Basic field validation
    if not miner_content.url or not miner_content.title or not miner_content.content:
        return ValidationResult(
            is_valid=False,
            reason="Missing required fields (url, title, or content)",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # URL must match exactly
    if miner_content.url != validator_scraped_content.url:
        return ValidationResult(
            is_valid=False,
            reason="URL mismatch between submitted and validator scraped content",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Title validation with similarity check
    title_similarity = calculate_text_similarity(
        miner_content.title, 
        validator_scraped_content.title
    )
    if title_similarity < 0.7:  # 70% similarity threshold
        return ValidationResult(
            is_valid=False,
            reason=f"Title similarity too low: {title_similarity:.2f} < 0.70",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Content validation - randomly sample portions for comparison
    content_similarity = validate_content_with_random_sampling(
        miner_content.content,
        validator_scraped_content.content
    )
    
    if content_similarity < 0.5:  # 50% similarity threshold for content
        return ValidationResult(
            is_valid=False,
            reason=f"Content similarity too low: {content_similarity:.2f} < 0.50",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Timestamp validation
    if miner_content.timestamp > dt.datetime.now(dt.timezone.utc):
        return ValidationResult(
            is_valid=False,
            reason="Timestamp is in the future",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # All validations passed
    return ValidationResult(
        is_valid=True,
        reason="Web content validation successful",
        content_size_bytes_validated=entity.content_size_bytes,
    )


def validate_content_with_random_sampling(
    miner_content: str, 
    validator_content: str,
    num_samples: int = 5,
    sample_size: int = 50
) -> float:
    """
    Validate content by randomly sampling portions and checking similarity.
    Similar to SN22's approach of random validation checks.
    """
    if not miner_content or not validator_content:
        return 0.0
    
    # Split content into words
    miner_words = miner_content.split()
    validator_words = validator_content.split()
    
    if len(miner_words) < sample_size or len(validator_words) < sample_size:
        # For short content, check overall similarity
        return calculate_text_similarity(miner_content, validator_content)
    
    similarities = []
    
    for _ in range(num_samples):
        # Randomly select a starting position in miner content
        max_start = len(miner_words) - sample_size
        if max_start <= 0:
            continue
            
        start_pos = random.randint(0, max_start)
        miner_sample = ' '.join(miner_words[start_pos:start_pos + sample_size])
        
        # Find best matching section in validator content
        best_similarity = 0.0
        for i in range(len(validator_words) - sample_size + 1):
            validator_sample = ' '.join(validator_words[i:i + sample_size])
            similarity = calculate_text_similarity(miner_sample, validator_sample)
            best_similarity = max(best_similarity, similarity)
        
        similarities.append(best_similarity)
    
    # Return average similarity across all samples
    return sum(similarities) / len(similarities) if similarities else 0.0


def calculate_text_similarity(text1: str, text2: str) -> float:
    """Calculate similarity between two text strings using word overlap."""
    if not text1 or not text2:
        return 0.0
    
    # Normalize and tokenize
    words1 = set(text1.lower().split())
    words2 = set(text2.lower().split())
    
    if not words1 or not words2:
        return 0.0
    
    # Calculate Jaccard similarity (intersection over union)
    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))
    
    return intersection / union if union > 0 else 0.0


def validate_random_url_subset(entities: List[DataEntity], sample_ratio: float = 0.3) -> List[DataEntity]:
    """
    Randomly select a subset of entities for validation, similar to SN22 approach.
    """
    if not entities:
        return []
    
    sample_size = max(1, int(len(entities) * sample_ratio))
    return random.sample(entities, min(sample_size, len(entities)))


def normalize_url(url: str) -> str:
    """Normalize URL for comparison (remove trailing slash, fragments, etc.)"""
    if not url:
        return url
    
    try:
        parsed = urlparse(url)
        # Remove fragment and trailing slash
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
        if parsed.query:
            normalized += f"?{parsed.query}"
        return normalized
    except Exception:
        return url


def extract_domain(url: str) -> str:
    """Extract domain from URL for categorization."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain.lower()
    except Exception:
        return ""


def validate_url_accessibility(url: str) -> bool:
    """Basic URL format validation."""
    if not url:
        return False
    
    try:
        parsed = urlparse(url)
        return (
            parsed.scheme in ('http', 'https') and 
            bool(parsed.netloc) and
            '.' in parsed.netloc  # Must have at least one dot in domain
        )
    except Exception:
        return False