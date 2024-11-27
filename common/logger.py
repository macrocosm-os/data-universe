from loguru import logger
import sys


def setup_logger():
    """Initial logger setup"""
    # Remove default handler
    logger.remove()

    # Add console handler with TRACE level (lowest)
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="TRACE"  # Changed from INFO to TRACE to capture all levels
    )

    # Add file handler
    logger.add(
        "logs/file_{time}.log",
        rotation="500 MB",
        retention="10 days",
        level="TRACE"  # Changed from DEBUG to TRACE
    )


# Setup logger once when module is imported
setup_logger()

# Just export logger directly
__all__ = ['logger']