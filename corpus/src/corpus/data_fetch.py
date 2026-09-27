from dataclasses import dataclass
import requests


@dataclass(frozen=True)
class NetworkRailCredentials:
    username: str
    password: str


def get_corpus_url() -> str:
    return "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS"


def create_requests_session() -> requests.Session:
    raise NotImplementedError


def get_network_rail_credentials_from_secret_manager(
    project_id: str,
    secret_id: str,
) -> NetworkRailCredentials:
    raise NotImplementedError


def fetch_latest_corpus_file(
    credentials: NetworkRailCredentials,
) -> bytes:
    response = requests.get(
        url=get_corpus_url(),
        auth=(credentials.username, credentials.password)
    )

    response.raise_for_status()

    return response.content
