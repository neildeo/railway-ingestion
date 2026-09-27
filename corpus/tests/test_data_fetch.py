import json
from unittest.mock import Mock, patch

import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.api_core import exceptions as google_exceptions

from corpus.data_fetch import (
    NetworkRailCredentials,
    fetch_latest_corpus_file,
    get_corpus_url,
    create_requests_session,
    get_network_rail_credentials_from_secret_manager,
)


EXPECTED_CORPUS_URL = (
    "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate"
    "?type=CORPUS"
)

MAX_RETRIES = 5

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}


def test_get_corpus_url_returns_corpus_endpoint() -> None:
    assert get_corpus_url() == EXPECTED_CORPUS_URL


def test_requests_session_has_expected_retry_policy() -> None:
    session = create_requests_session()

    adapter = session.get_adapter("https://")

    assert isinstance(adapter, HTTPAdapter)
    assert isinstance(adapter.max_retries, Retry)

    retry = adapter.max_retries

    assert retry.total == 5
    assert retry.status_forcelist == RETRYABLE_STATUS_CODES
    assert retry.allowed_methods == frozenset({"GET"})


def test_successful_fetch_returns_exact_response_bytes() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    response = Mock()
    response.content = b'{"TIPLOCDATA":[]}'
    response.raise_for_status.return_value = None

    session = Mock()
    session.get.return_value = response

    with patch(
            "corpus.data_fetch.create_requests_session",
            return_value=session,
    ) as mock_get_session:
        result = fetch_latest_corpus_file(credentials)

    assert result == b'{"TIPLOCDATA":[]}'
    response.raise_for_status.assert_called_once_with()
    session.get.assert_called_once()


def test_non_transient_http_errors_are_not_retried() -> None:
    credentials = NetworkRailCredentials(
        username="bad-user",
        password="bad-password",
    )

    non_transient_status_codes = [401, 403, 404]

    for status_code in non_transient_status_codes:
        response = Mock()
        response.status_code = status_code

        http_error = requests.HTTPError(response=response)
        response.raise_for_status.side_effect = http_error

        session = Mock()
        session.get.return_value = response

        with patch(
            "corpus.data_fetch.create_requests_session",
            return_value=session,
        ):
            with pytest.raises(requests.HTTPError) as exc_info:
                fetch_latest_corpus_file(credentials)

        assert exc_info.value.response is not None
        assert exc_info.value.response.status_code == status_code
        session.get.assert_called_once()


def test_fetch_uses_configured_requests_session() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    response = Mock()
    response.content = b'{"TIPLOCDATA":[]}'
    response.raise_for_status.return_value = None

    session = Mock()
    session.get.return_value = response

    with patch(
        "corpus.data_fetch.create_requests_session",
        return_value=session,
    ) as mock_get_session:
        result = fetch_latest_corpus_file(credentials)

    mock_get_session.assert_called_once_with()
    session.get.assert_called_once()
    assert result == b'{"TIPLOCDATA":[]}'


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
