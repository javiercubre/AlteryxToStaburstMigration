"""
Logging configuration for the Alteryx to DBT Documentation Generator.

LOW-04 fix: Replaces print() statements with Python logging module.

Usage:
    from logging_config import setup_logging, get_logger

    # In main.py (call once at startup)
    setup_logging(verbose=args.verbose, quiet=args.quiet, log_file=args.log_file)

    # In any module
    logger = get_logger(__name__)
    logger.info("Processing workflow...")
    logger.debug("Detailed info...")
    logger.warning("Potential issue...")
    logger.error("Something failed")
"""
import logging
import sys
from pathlib import Path
from typing import Optional


# Module-level logger registry
_loggers: dict = {}

# Default format
DEFAULT_FORMAT = "%(message)s"
VERBOSE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging(
    verbose: bool = False,
    quiet: bool = False,
    log_file: Optional[str] = None,
    log_format: Optional[str] = None
) -> None:
    """Configure logging for the application.

    Args:
        verbose: If True, set level to DEBUG and use verbose format.
        quiet: If True, set level to WARNING (only warnings and errors).
        log_file: Optional path to a log file. If provided, logs are written to file.
        log_format: Optional custom format string. If not provided, uses defaults.

    Note:
        If both verbose and quiet are True, verbose takes precedence.
    """
    # Determine log level
    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.WARNING
    else:
        level = logging.INFO

    # Determine format
    if log_format:
        fmt = log_format
    elif verbose:
        fmt = VERBOSE_FORMAT
    else:
        fmt = DEFAULT_FORMAT

    # Create formatter
    formatter = logging.Formatter(fmt)

    # Configure root logger for our package
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    root_logger.handlers = []

    # Console handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(str(file_path), encoding='utf-8')
        file_handler.setLevel(level)
        # Always use verbose format for file output
        file_handler.setFormatter(logging.Formatter(VERBOSE_FORMAT))
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger for the specified module.

    Args:
        name: The module name (typically __name__).

    Returns:
        A configured Logger instance.
    """
    if name not in _loggers:
        _loggers[name] = logging.getLogger(name)
    return _loggers[name]


def set_level(level: int) -> None:
    """Set the logging level for all loggers.

    Args:
        level: A logging level (e.g., logging.DEBUG, logging.INFO).
    """
    logging.getLogger().setLevel(level)
    for logger in _loggers.values():
        logger.setLevel(level)
