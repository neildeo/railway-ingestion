from dataclasses import dataclass


@dataclass(frozen=True)
class NetworkRailCredentials:
    username: str
    password: str


def get_corpus_url() -> str:
    raise NotImplementedError


def get_network_rail_credentials_from_secret_manager(
    project_id: str,
    secret_id: str,
) -> NetworkRailCredentials:
    raise NotImplementedError


def fetch_latest_corpus_file(
    credentials: NetworkRailCredentials,
) -> bytes:
    raise NotImplementedError
