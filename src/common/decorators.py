import time
import logging
import functools

# Start logger
logger = logging.getLogger(__name__)

def performance_counter(func):
    """Counts performance of the function"""
    @functools.wraps(func)
    def wrapper_performance_counter(*args, **kwargs):
        start_time = time.perf_counter()
        response = func(*args, **kwargs)
        duration = round(time.perf_counter() - start_time, 2)
        if duration < 600:
            logger.info(f"Process {func.__name__!r} finished with duration -> {duration} seconds")
        else:
            duration = round(duration / 60, 2)  # counting by minutes
            logger.info(f"Process {func.__name__!r} finished with duration -> {duration} minutes")
        return response

    return wrapper_performance_counter