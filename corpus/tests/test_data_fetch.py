import json
from unittest.mock import Mock, patch

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

    response = Mock()
    response.content = b'{"TIPLOCDATA":[]}'
    response.raise_for_status.return_value = None

    with patch(
        "corpus.data_fetch.requests.get",
        return_value=response,
    ):
        result = fetch_latest_corpus_file(credentials)

    assert result == b'{"TIPLOCDATA":[]}'
    response.raise_for_status.assert_called_once_with()


def test_authentication_failure_is_not_retried() -> None:
    credentials = NetworkRailCredentials(
        username="bad-user",
        password="bad-password",
    )

    response = Mock()
    response.status_code = 401

    http_error = requests.HTTPError(response=response)
    response.raise_for_status.side_effect = http_error

    with patch(
        "corpus.data_fetch.requests.get",
        return_value=response,
    ) as mock_get:
        with pytest.raises(requests.HTTPError) as exc_info:
            fetch_latest_corpus_file(credentials)

    assert exc_info.value.response is not None
    assert exc_info.value.response.status_code == 401
    mock_get.assert_called_once()


def test_not_found_is_not_retried() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    response = Mock()
    response.status_code = 404

    http_error = requests.HTTPError(response=response)
    response.raise_for_status.side_effect = http_error

    with patch(
        "corpus.data_fetch.requests.get",
        return_value=response,
    ) as mock_get:
        with pytest.raises(requests.HTTPError) as exc_info:
            fetch_latest_corpus_file(credentials)

    assert exc_info.value.response is not None
    assert exc_info.value.response.status_code == 404
    mock_get.assert_called_once()


def test_server_error_is_retried_then_succeeds() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    failed_response = Mock()
    failed_response.status_code = 503
    failed_response.raise_for_status.side_effect = requests.HTTPError(
        response=failed_response
    )

    success_response = Mock()
    success_response.status_code = 200
    success_response.raise_for_status.return_value = None
    success_response.content = b'{"TIPLOCDATA":[]}'

    with patch(
        "corpus.data_fetch.requests.get",
        side_effect=[failed_response, success_response],
    ) as mock_get:
        result = fetch_latest_corpus_file(credentials)

    assert result == b'{"TIPLOCDATA":[]}'
    assert mock_get.call_count == 2


def test_timeout_is_retried_then_succeeds() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    success_response = Mock()
    success_response.status_code = 200
    success_response.raise_for_status.return_value = None
    success_response.content = b'{"TIPLOCDATA":[]}'

    with patch(
        "corpus.data_fetch.requests.get",
        side_effect=[
            requests.Timeout(),
            success_response,
        ],
    ) as mock_get:
        result = fetch_latest_corpus_file(credentials)

    assert result == b'{"TIPLOCDATA":[]}'
    assert mock_get.call_count == 2


def test_transient_failure_exhausting_retries_fails() -> None:
    credentials = NetworkRailCredentials(
        username="test-user",
        password="test-password",
    )

    response = Mock()
    response.status_code = 503

    http_error = requests.HTTPError(response=response)
    response.raise_for_status.side_effect = http_error

    with patch(
        "corpus.data_fetch.requests.get",
        return_value=response,
    ) as mock_get:
        with pytest.raises(requests.HTTPError) as exc_info:
            fetch_latest_corpus_file(credentials)

    assert exc_info.value.response is not None
    assert exc_info.value.response.status_code == 503
    assert mock_get.call_count == 6
