from datetime import datetime, timezone
from corpus.core import sync_corpus
from corpus.data_fetch import fetch_latest_corpus_file
from corpus.storage import crc32c_checksum, get_object_state, upload_object
import logging
from corpus.logging_config import configure_logging

PROJECT_ID = "railway-analytics-508615"
SECRET_ID = "network-rail-credentials"

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()

    acquisition_date = datetime.now(timezone.utc).date()

    logger.info(
        "Starting CORPUS ingestion for acquisition date %s",
        acquisition_date,
    )

    sync_corpus(
        acquisition_date=acquisition_date,
        project_id=PROJECT_ID,
        secret_id=SECRET_ID,
        fetch_latest_corpus_file=fetch_latest_corpus_file,
        crc32c_checksum=crc32c_checksum,
        get_object_state=get_object_state,
        upload_object=upload_object,
    )

    logger.info(
        "CORPUS ingestion completed successfully for acquisition date %s",
        acquisition_date,
    )


if __name__ == "__main__":
    main()
