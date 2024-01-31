import threading


concurrent_lock = threading.RLock()
concurrent_count = 0


def get_and_increment_count():
    global concurrent_count
    with concurrent_lock:
        concurrent_count += 1
        return concurrent_count


def decrement_count():
    global concurrent_count
    with concurrent_lock:
        concurrent_count -= 1
