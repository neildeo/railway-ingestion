from datetime import datetime, timezone


def main() -> None:
    acquired_date = datetime.now(timezone.utc).date()

    # Real adapters will be wired in here next.
    raise NotImplementedError


if __name__ == "__main__":
    main()
