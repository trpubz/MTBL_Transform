import pytest
import pandas as pd

from mtbl_iokit.read import read

from app.src.mtbl_globals import ETLType
from app.src import cleaner
from app.src.loader import Loader
from app.src.keymap import KeyMap
from tests.fixtures.mock_helper import mock_savant, mock_fangraphs


class TestCleaner:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        self.combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")

    def test_clean_hitters(self):
        cleaned_hitters = cleaner.clean_hitters(self.combined_bats, ETLType.PRE_SZN)
        assert isinstance(cleaned_hitters, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in cleaned_hitters.columns.tolist()
        assert cleaned_hitters.iloc[0]["wRAA"] >= cleaned_hitters.iloc[1]["wRAA"]
        assert "SBN" in cleaned_hitters.columns.tolist()

    def test_clean_pitchers(self):
        cleaned_sps, cleaned_rps = cleaner.clean_pitchers(self.combined_arms, ETLType.PRE_SZN)
        assert isinstance(cleaned_sps, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in cleaned_sps.columns.tolist()
        assert cleaned_sps.iloc[0]["FIP"] <= cleaned_sps.iloc[1]["FIP"]
        assert "SVHD" in cleaned_rps.columns.tolist()
