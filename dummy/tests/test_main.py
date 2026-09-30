from dummy.__main__ import main
import pytest


def test_main_prints_message(capsys) -> None:
    with pytest.raises(RuntimeError, match="Intentional failure for alerting test"):
        main()
