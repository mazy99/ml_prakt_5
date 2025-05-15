import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pytest
import pandas as pd
import os
from src.storage import save_to_parquet, load_from_parquet

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "destination": ["Moscow", "London"],
        "flight_number": ["123", "456"],
        "aircraft_type": ["Boeing 737", "Airbus A320"]
    })

def test_save_and_load(sample_data, tmp_path):
    test_file = tmp_path / "test.parquet"
    save_to_parquet(sample_data, test_file)
    assert os.path.exists(test_file)
    
    loaded = load_from_parquet(test_file)
    pd.testing.assert_frame_equal(sample_data, loaded)