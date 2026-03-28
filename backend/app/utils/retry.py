import logging
import time
from typing import Callable, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


def retry_with_backoff(
    func: Callable[..., T],
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    *args,
    **kwargs,
) -> T:
    """
    Retry a function with exponential backoff.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay on each retry
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        Result from func

    Raises:
        Exception: Last exception if all retries fail
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            if attempt > 0:
                log.info(f"Retry successful on attempt {attempt + 1}")
            return result
        except Exception as exc:
            last_exception = exc
            if attempt < max_retries:
                log.warning(
                    f"Attempt {attempt + 1} failed: {exc}. Retrying in {delay}s..."
                )
                time.sleep(delay)
                delay *= backoff_factor
            else:
                log.error(
                    f"All {max_retries + 1} attempts failed. Last error: {exc}"
                )

    raise last_exception
