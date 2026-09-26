from dataclasses import dataclass
import requests

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


def get_network_rail_credentials_from_secret_manager(
    project_id: str,
    secret_id: str,
) -> NetworkRailCredentials:
    raise NotImplementedError


def fetch_latest_corpus_file(
    credentials: NetworkRailCredentials,
) -> bytes:
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(
                url=get_corpus_url(),
                auth=(credentials.username, credentials.password)
            )

            response.raise_for_status()

            return response.content

        except requests.Timeout as e:
            if attempt == MAX_RETRIES:
                raise
            continue

        except requests.HTTPError as e:
            assert e.response is not None
            if attempt == MAX_RETRIES:
                raise

            if e.response.status_code in RETRYABLE_STATUS_CODES:
                continue
            else:
                raise

    raise RuntimeError("Unreachable")
