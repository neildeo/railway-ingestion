from dummy.__main__ import main


def test_main_prints_message(capsys) -> None:
    main()

    captured = capsys.readouterr()

    assert captured.out == "Hello from railway-ingestion\n"
