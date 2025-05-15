import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pytest
from src.validators import validate_flight

@pytest.mark.parametrize("dest, num, aircraft, expected", [
    ("Moscow", "123", "Boeing 737", True),
    ("", "123", "Airbus A320", False),
    ("London", "ABC", "Boeing 747", False),
    ("Paris", "456", "", False)
])
def test_validate_flight(dest, num, aircraft, expected):
    assert validate_flight(dest, num, aircraft) == expected