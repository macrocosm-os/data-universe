from typing import Any, Callable, Iterable
import time

def get_only_element_matching_filter(iterable: Iterable[Any], filter: Callable[[Any], bool]) -> Any:
    """Returns the only element in the iterable that matches the filter, or raises an exception if there are zero or more than one elements."""
    results = [x for x in iterable if filter(x)]
    if len(results) != 1:
        raise Exception(f"Expected exactly one element matching filter, but found {len(results)}")
    return results[0]

def wait_for_condition(condition: Callable[[], bool], timeout: float = 10.0):
    """Waits until the provided condition is true, or until the timeout is reached."""
    start_time = time.time()
    while not condition():
        if time.time() - start_time > timeout:
            raise Exception("Timed out waiting for condition to be true.")
        time.sleep(0.1)