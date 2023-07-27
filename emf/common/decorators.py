import time
import logging
import functools

# Start logger
logger = logging.getLogger(__name__)

def performance_counter(units='seconds'):
    """Counts performance of the function"""
    def decorator_performance_counter(func):
        @functools.wraps(func)
        def wrapper_performance_counter(*args, **kwargs):
            start_time = time.perf_counter()
            response = func(*args, **kwargs)
            duration = round(time.perf_counter() - start_time, 2)
            if units == 'minutes':
                duration = round(duration / 60, 2)  # counting by minutes
            logger.info(f"Process {func.__name__!r} finished with duration: {duration} {units}", extra={'numeric_value': duration})
            return response
        return wrapper_performance_counter
    return decorator_performance_counter
