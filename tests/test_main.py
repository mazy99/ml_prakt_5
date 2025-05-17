#!/usr/bin/env python3

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from unittest.mock import patch

from src.main import main


def test_main_add_flight(capsys):
    testargs = ["main.py", "add", "Moscow", "123", "Boeing 737"]
    with patch("sys.argv", testargs):
        main()
    captured = capsys.readouterr()
    assert "Рейс 123 в Moscow добавлен" in captured.out
