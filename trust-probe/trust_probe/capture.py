"""Capture raw Network Rail TRUST STOMP messages."""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

import google.auth
import stomp
from google.cloud import secretmanager


HOST = "publicdatafeeds.networkrail.co.uk"
PORT = 61618
DESTINATION = "/topic/TRAIN_MVT_ALL_TOC"
SECRET_ID = "network-rail-credentials"
SUBSCRIPTION_ID = "trust-probe"

LOGGER = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_credentials() -> tuple[str, str]:
    """Retrieve credentials using GCP Application Default Credentials."""
    _, project_id = google.auth.default()

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or project_id
    if not project_id:
        raise RuntimeError(
            "Could not determine GCP project ID. "
            "Set GOOGLE_CLOUD_PROJECT."
        )

    client = secretmanager.SecretManagerServiceClient()
    name = (
        f"projects/{project_id}/secrets/{SECRET_ID}/versions/latest"
    )

    response = client.access_secret_version(request={"name": name})
    credentials = json.loads(response.payload.data.decode("utf-8"))

    return credentials["username"], credentials["password"]


class CaptureListener(stomp.ConnectionListener):
    """Record every received MESSAGE frame as one NDJSON line."""

    def __init__(self, output):
        self.output = output
        self.count = 0
        self.stopped = threading.Event()
        self.failure: str | None = None
        self._lock = threading.Lock()

    def on_message(self, frame):
        record = {
            "received_at": utc_now().isoformat(),
            "headers": frame.headers,
            "body": frame.body,
        }

        try:
            with self._lock:
                self.output.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )
                self.output.flush()
                self.count += 1
        except Exception as exc:
            self.failure = f"Failed to write capture: {exc}"
            LOGGER.error("%s", self.failure)
            self.stopped.set()

    def on_error(self, frame):
        self.failure = f"STOMP ERROR frame: {frame.body}"
        LOGGER.error("%s", self.failure)
        self.stopped.set()

    def on_disconnected(self):
        LOGGER.warning("STOMP connection disconnected")
        self.stopped.set()

    def on_heartbeat_timeout(self):
        self.failure = "STOMP heartbeat timeout"
        LOGGER.error("%s", self.failure)
        self.stopped.set()


def capture(minutes: float, output_path: Path) -> int:
    """Capture messages for the requested observation period."""
    if minutes <= 0:
        raise ValueError("Capture duration must be positive")

    username, password = get_credentials()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    connection = stomp.Connection12(
        host_and_ports=[(HOST, PORT)],
        heartbeats=(10000, 10000),
    )

    with output_path.open("w", encoding="utf-8") as output:
        listener = CaptureListener(output)
        connection.set_listener("capture", listener)

        try:
            LOGGER.info("Connecting to %s:%s", HOST, PORT)
            connection.connect(username, password, wait=True)

            connection.subscribe(
                destination=DESTINATION,
                id=SUBSCRIPTION_ID,
                ack="auto",
            )

            LOGGER.info(
                "Subscribed to %s; capturing for %.2f minutes",
                DESTINATION,
                minutes,
            )

            # Wait until the observation window ends, or the
            # listener reports a failure/disconnection.
            interrupted = listener.stopped.wait(minutes * 60)

            if listener.failure:
                raise RuntimeError(listener.failure)

            if interrupted:
                raise RuntimeError(
                    "Connection ended before capture duration elapsed"
                )

        except KeyboardInterrupt:
            LOGGER.info("Capture interrupted by user")
        finally:
            if connection.is_connected():
                connection.disconnect()

        LOGGER.info(
            "Captured %d STOMP messages to %s",
            listener.count,
            output_path,
        )
        return listener.count
