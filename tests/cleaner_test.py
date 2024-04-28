import pytest
import pandas as pd

from mtbl_iokit.read import read

from app.src.mtbl_globals import ETLType
from app.src.cleaner import Cleaner
from app.src.loader import Loader
from app.src.keymap import KeyMap
from tests.fixtures.mock_helper import mock_savant, mock_fangraphs


class TestCleaner:
    @pytest.fixture(autouse=True)
    def setup(self):
        combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")
        self.cleaner = Cleaner(etl_type=ETLType.PRE_SZN, bats=combined_bats, arms=combined_arms)
        self.cleaned_bats = self.cleaner.clean_hitters()
        self.cleaned_sps, self.cleaned_rps = self.cleaner.clean_pitchers()

    def test_clean_hitters(self):
        assert isinstance(self.cleaned_bats, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in self.cleaned_bats.columns.tolist()
        assert self.cleaned_bats.iloc[0]["wRAA"] >= self.cleaned_bats.iloc[1]["wRAA"]
        assert "SBN" in self.cleaned_bats.columns.tolist()

    def test_clean_pitchers(self):
        assert isinstance(self.cleaned_sps, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in self.cleaned_sps.columns.tolist()
        assert self.cleaned_sps.iloc[0]["FIP"] <= self.cleaned_sps.iloc[1]["FIP"]
        assert "SVHD" in self.cleaned_rps.columns.tolist()
