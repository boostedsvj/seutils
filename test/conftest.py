import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--integration", action="store_true", default=False, help="run real integration tests"
        )

def pytest_configure(config):
    config.addinivalue_line("markers", "real_integration: mark test as a real integration test")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        # --integration given in cli: do not skip real integration tests
        return
    skip_real_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "real_integration" in item.keywords:
            item.add_marker(skip_real_integration)