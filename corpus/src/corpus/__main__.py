from datetime import datetime, timezone
from corpus.core import sync_corpus
from corpus.data_fetch import fetch_latest_corpus_file
from corpus.storage import crc32c_checksum, get_object_state, upload_object

PROJECT_ID = "railway-analytics-508615"
SECRET_ID = "network-rail-credentials"


def main() -> None:
    acquisition_date = datetime.now(timezone.utc).date()

    sync_corpus(
        acquisition_date=acquisition_date,
        project_id=PROJECT_ID,
        secret_id=SECRET_ID,
        fetch_latest_corpus_file=fetch_latest_corpus_file,
        crc32c_checksum=crc32c_checksum,
        get_object_state=get_object_state,
        upload_object=upload_object,
    )


if __name__ == "__main__":
    main()
