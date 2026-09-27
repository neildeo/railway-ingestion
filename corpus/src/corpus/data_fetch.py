from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import secretmanager

MAX_RETRIES = 5

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}


@dataclass(frozen=True)
class NetworkRailCredentials:
    username: str
    password: str


def get_corpus_url() -> str:
    return "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS"


def create_requests_session() -> requests.Session:
    retry_policy = Retry(
        total=MAX_RETRIES,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=RETRYABLE_STATUS_CODES,
    )
    s = requests.Session()
    s.mount(
        prefix="https://",
        adapter=HTTPAdapter(max_retries=retry_policy),
    )

    return s


def get_network_rail_credentials_from_secret_manager(
    project_id: str,
    secret_id: str,
) -> NetworkRailCredentials:
    raise NotImplementedError


def fetch_latest_corpus_file(
    credentials: NetworkRailCredentials,
) -> bytes:
    session = create_requests_session()

    response = session.get(
        url=get_corpus_url(),
        auth=(credentials.username, credentials.password)
    )

    response.raise_for_status()

    return response.content
