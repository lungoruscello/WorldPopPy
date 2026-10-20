import socket

import pytest


# --- Fixtures ---

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """
    Fixture that blocks all network access (DNS lookups and connections)
    in unit tests.

    Unit tests must fake every external service. This guard ensures that a
    fake which fails to intercept (e.g., due to a wrong patch target) makes
    the test fail immediately, rather than sending real traffic to WorldPop
    or Nominatim. (`pytest.fail` raises a BaseException, so the library's own
    `except Exception` blocks cannot swallow it.)
    """
    def _blocked(*args, **kwargs):
        pytest.fail(f"Unit tests must not access the network (attempted: {args!r}).")

    monkeypatch.setattr(socket, "getaddrinfo", _blocked)
    monkeypatch.setattr(socket.socket, "connect", _blocked)
