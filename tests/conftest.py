"""
Shared pytest configuration and fixtures.
"""

import builtins
import errno
import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

# Add the project root to the Python path to ensure imports work
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(autouse=True, scope="session")
def clean_auth_environment():
    """Keep ambient auth variables from flipping test servers into auth mode.

    Tests that exercise authentication set these explicitly per test.
    """
    for name in ("YT2RSS_PASSWORD", "YT2RSS_USERNAME", "YT2RSS_SECRET_KEY"):
        os.environ.pop(name, None)
    yield
    for name in ("YT2RSS_PASSWORD", "YT2RSS_USERNAME", "YT2RSS_SECRET_KEY"):
        os.environ.pop(name, None)


@pytest.fixture(scope="session")
def project_root_path():
    """Provide the project root path."""
    return Path(__file__).parent.parent


@pytest.fixture
def temp_directory():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_logging():
    """Mock the logging setup to avoid file creation during tests."""
    with patch("src.web_server.setup_logging") as mock_log:
        mock_log.return_value.info = lambda x: None
        mock_log.return_value.error = lambda x: None
        mock_log.return_value.warning = lambda x: None
        yield mock_log


@pytest.fixture
def mock_atexit():
    """Mock atexit.register to avoid cleanup issues during tests."""
    with patch("atexit.register") as mock_exit:
        yield mock_exit


@pytest.fixture
def unreadable_files() -> Iterator[Callable[[Path], None]]:
    """Make reads of registered paths fail with ``PermissionError``.

    Only builtin ``open`` calls on exactly those paths fail; every other
    file (and ``Path.read_bytes`` used by assertions) works normally, in
    every thread. Unlike ``chmod`` this also works when tests run as root.

    Yields:
        A callable registering one path whose reads must fail.
    """
    blocked: set[str] = set()
    real_open = builtins.open

    def guarded_open(file: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(file, (str, os.PathLike)) and os.path.abspath(file) in blocked:
            raise PermissionError(errno.EACCES, "Permission denied", os.fspath(file))
        return real_open(file, *args, **kwargs)

    def block(path: Path) -> None:
        blocked.add(os.path.abspath(path))

    with patch("builtins.open", guarded_open):
        yield block
