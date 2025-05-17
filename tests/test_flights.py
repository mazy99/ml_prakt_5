#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pytest
import pandas as pd
from src.flights import add_flight, show_flights

@pytest.fixture
def empty_df():
    return pd.DataFrame(columns=["destination", "flight_number", "aircraft_type"])

def test_add_flight_valid(empty_df):
    df = add_flight(empty_df, "Moscow", "123", "Boeing 737")
    assert len(df) == 1
    assert df.iloc[0].to_dict() == {
        "destination": "Moscow",
        "flight_number": "123",
        "aircraft_type": "Boeing 737"
    }

def test_show_flights(empty_df, capsys):
    show_flights(empty_df)
    captured = capsys.readouterr()
    assert "Список рейсов пуст" in captured.out