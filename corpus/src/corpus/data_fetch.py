from dataclasses import dataclass
import requests
from json import JSONDecoder
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import secretmanager
from google.api_core.retry import Retry as GoogleRetry

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
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Add retry policy later
    secret = client.access_secret_version(
        name=secret_name,
        retry=GoogleRetry(
            initial=1.0,
            maximum=8.0,
            multiplier=2.0,
            timeout=30.0,
        )
    )

    payload_dict: dict[str, str] = JSONDecoder().decode(
        secret.payload.data.decode())

    try:
        username = payload_dict["username"]
    except KeyError:
        raise ValueError("Secret payload is missing username")

    try:
        password = payload_dict["password"]
    except KeyError:
        raise ValueError("Secret payload is missing password")

    return NetworkRailCredentials(username, password)


def fetch_latest_corpus_file(
    project_id: str,
    secret_id: str,
) -> bytes:
    credentials = get_network_rail_credentials_from_secret_manager(
        project_id,
        secret_id,
    )

    session = create_requests_session()

    response = session.get(
        url=get_corpus_url(),
        auth=(credentials.username, credentials.password)
    )

    response.raise_for_status()

    return response.content
