import json
from unittest.mock import Mock

import pytest
import requests
from google.api_core import exceptions as google_exceptions

from corpus.data_fetch import (
    NetworkRailCredentials,
    fetch_latest_corpus_file,
    get_corpus_url,
    get_network_rail_credentials_from_secret_manager,
)


EXPECTED_CORPUS_URL = (
    "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate"
    "?type=CORPUS"
)


def test_get_corpus_url_returns_corpus_endpoint() -> None:
    assert get_corpus_url() == EXPECTED_CORPUS_URL


def test_secret_manager_credentials_are_parsed() -> None:
    credentials = get_network_rail_credentials_from_secret_manager(
        project_id="test-project",
        secret_id="network-rail-credentials",
    )

    assert credentials == NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )


def test_secret_manager_missing_secret_fails() -> None:
    with pytest.raises(google_exceptions.NotFound):
        get_network_rail_credentials_from_secret_manager(
            project_id="test-project",
            secret_id="missing-secret",
        )


def test_secret_manager_permission_denied_fails() -> None:
    with pytest.raises(google_exceptions.PermissionDenied):
        get_network_rail_credentials_from_secret_manager(
            project_id="test-project",
            secret_id="network-rail-credentials",
        )


def test_secret_manager_malformed_secret_fails() -> None:
    with pytest.raises(ValueError):
        get_network_rail_credentials_from_secret_manager(
            project_id="test-project",
            secret_id="malformed-secret",
        )


def test_successful_fetch_returns_exact_response_bytes() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    result = fetch_latest_corpus_file(credentials)

    assert result == b'{"TIPLOCDATA":[]}'


def test_authentication_failure_is_not_retried() -> None:
    credentials = NetworkRailCredentials(
        username="bad-user",
        password="bad-password",
    )

    with pytest.raises(requests.HTTPError):
        fetch_latest_corpus_file(credentials)


def test_not_found_is_not_retried() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    with pytest.raises(requests.HTTPError):
        fetch_latest_corpus_file(credentials)


def test_server_error_is_retried_then_succeeds() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    result = fetch_latest_corpus_file(credentials)

    assert result == b'{"TIPLOCDATA":[]}'


def test_timeout_is_retried_then_succeeds() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    result = fetch_latest_corpus_file(credentials)

    assert result == b'{"TIPLOCDATA":[]}'


def test_transient_failure_exhausting_retries_fails() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    with pytest.raises(requests.RequestException):
        fetch_latest_corpus_file(credentials)
