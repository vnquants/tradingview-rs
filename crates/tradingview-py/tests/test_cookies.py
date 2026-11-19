"""Offline tests for exporting and re-importing cookie sessions."""

import json
from collections.abc import Callable

import pytest
from tradingview import DataServer, TradingViewClient
from tradingview.exceptions import AuthenticationError

FAKE_COOKIES: dict[str, object] = {
    "id": 42,
    "username": "alice",
    "private_channel": "private-channel",
    "auth_token": "header.payload.signature",
    "session": "fake-session-id",
    "session_signature": "fake-session-signature",
    "session_hash": "fake-session-hash",
    "device_token": "fake-device-token",
}


def test_from_cookies_round_trip() -> None:
    client = TradingViewClient.from_cookies(json.dumps(FAKE_COOKIES))
    assert json.loads(client.export_cookies()) == FAKE_COOKIES
    assert client.username == "alice"
    assert client.auth_token == "header.payload.signature"
    assert client.is_authenticated is True
    assert client.server == DataServer.Data

    # A second hop keeps the exact JSON.
    again = TradingViewClient.from_cookies(client.export_cookies())
    assert again.export_cookies() == client.export_cookies()


def test_from_cookies_server_is_keyword_only() -> None:
    client = TradingViewClient.from_cookies(
        json.dumps(FAKE_COOKIES), server=DataServer.ProData
    )
    assert client.server == DataServer.ProData

    from_cookies: Callable[..., object] = TradingViewClient.from_cookies
    with pytest.raises(TypeError):
        from_cookies(json.dumps(FAKE_COOKIES), DataServer.ProData)
    with pytest.raises(TypeError):
        from_cookies(json.dumps(FAKE_COOKIES), server="prodata")


def test_from_cookies_accepts_minimal_session() -> None:
    client = TradingViewClient.from_cookies(
        json.dumps({"session": "s", "session_signature": "sig", "unknown": True})
    )
    exported = json.loads(client.export_cookies())
    assert exported["session"] == "s"
    assert exported["session_signature"] == "sig"
    assert exported["auth_token"] == ""
    # No auth token or username were supplied, so none are exposed.
    assert client.auth_token is None
    assert client.username is None
    assert client.is_authenticated is False


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        '["fake-session-id"]',
        json.dumps({**FAKE_COOKIES, "id": "fake-session-id"}),
        json.dumps({**FAKE_COOKIES, "session": None}),
    ],
)
def test_from_cookies_rejects_malformed_json(payload: str) -> None:
    with pytest.raises(ValueError, match="cookie JSON") as excinfo:
        TradingViewClient.from_cookies(payload)
    assert "fake-session-id" not in str(excinfo.value)


@pytest.mark.parametrize(
    "missing",
    [
        {"session": ""},
        {"session_signature": "   "},
    ],
)
def test_from_cookies_rejects_empty_session(missing: dict[str, str]) -> None:
    with pytest.raises(AuthenticationError, match="no session"):
        TradingViewClient.from_cookies(json.dumps({**FAKE_COOKIES, **missing}))


def test_export_cookies_rejects_clients_without_cookie_session() -> None:
    with pytest.raises(AuthenticationError, match="no cookie session"):
        TradingViewClient().export_cookies()
    with pytest.raises(AuthenticationError, match="no cookie session"):
        TradingViewClient(auth_token="token-only").export_cookies()
