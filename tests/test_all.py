import unittest


def create_test_suite():
    test_suite = unittest.TestSuite()

    # Find all tests in the current directory and subdirectories
    loader = unittest.TestLoader()
    suite = loader.discover("./tests", pattern="test_*.py")
    print(loader.errors)

    # Add the discovered tests to the test suite
    test_suite.addTest(suite)

    return test_suite


# TODO: Fix this.
# The tests fail because of a ModuleNotFoundError.
if __name__ == "__main__":
    suite = create_test_suite()

    # Run the tests
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
