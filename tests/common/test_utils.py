import functools
import time
import unittest

from common.utils import run_in_thread


class TestUtils(unittest.TestCase):
    def test_run_in_thread(self):
        def test_func(a: int, b: int):
            return a + b

        partial = functools.partial(test_func, 1, 2)

        result = run_in_thread(func=partial, ttl=5)
        self.assertEqual(3, result)

    def test_run_in_thread_timeout(self):
        def test_func(a: int, b: int):
            time.sleep(3)
            return a + b

        partial = functools.partial(test_func, 1, 2)

        with self.assertRaises(TimeoutError):
            result = run_in_thread(func=partial, ttl=1)

    def test_run_in_thread_no_return(self):
        def test_func(a: int, b: int):
            pass

        partial = functools.partial(test_func, 1, 2)

        result = run_in_thread(func=partial, ttl=5)
        self.assertIsNone(result)

    def test_run_in_thread_tuple_return(self):
        def test_func(a: int, b: int):
            return a, b

        partial = functools.partial(test_func, 1, 2)

        result = run_in_thread(func=partial, ttl=5)
        self.assertEqual((1, 2), result)

    def test_run_in_thread_exception(self):
        def test_func(a: int, b: int):
            raise ValueError()

        partial = functools.partial(test_func, 1, 2)

        with self.assertRaises(ValueError):
            result = run_in_thread(func=partial, ttl=5)


if __name__ == "__main__":
    unittest.main()
