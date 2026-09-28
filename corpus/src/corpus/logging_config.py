import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class CloudRunJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "time": datetime.fromtimestamp(
                record.created,
                tz=timezone.utc,
            ).isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "logger": record.name,
        }

        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload)


def configure_logging(
    level: int = logging.INFO,
) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CloudRunJsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
