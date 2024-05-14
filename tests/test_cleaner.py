import pytest
import pandas as pd

from app.src.mtbl_globals import ETLType
from app.src.cleaner import Cleaner


class TestCleaner:
    @pytest.fixture
    def setup_pre_szn(self):
        combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")
        self.cleaner = Cleaner(etl_type=ETLType.PRE_SZN, bats=combined_bats, arms=combined_arms)
        self.cleaned_bats = self.cleaner.clean_hitters()
        self.cleaned_sps, self.cleaned_rps = self.cleaner.clean_pitchers()

    @pytest.fixture
    def setup_reg_szn(self):
        combined_bats = pd.read_json("./tests/fixtures_reg_szn/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures_reg_szn/combined_arms.json")
        self.cleaner = Cleaner(etl_type=ETLType.REG_SZN, bats=combined_bats, arms=combined_arms)
        self.cleaned_bats = self.cleaner.clean_hitters()
        self.cleaned_sps, self.cleaned_rps = self.cleaner.clean_pitchers()

    def test_clean_hitters_pre_szn(self, setup_pre_szn):
        assert isinstance(self.cleaned_bats, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in self.cleaned_bats.columns.tolist()
        assert self.cleaned_bats.iloc[0]["proj_wRC+"] >= self.cleaned_bats.iloc[1]["proj_wRC+"]
        assert "proj_SBN" in self.cleaned_bats.columns.tolist()

    def test_clean_hitters_reg_szn(self, setup_reg_szn):
        assert isinstance(self.cleaned_bats, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in self.cleaned_bats.columns.tolist()
        assert self.cleaned_bats.iloc[0]["proj_wRC+"] >= self.cleaned_bats.iloc[1]["proj_wRC+"]
        assert "SBN" in self.cleaned_bats.columns.tolist()

    def test_clean_pitchers_pre_szn(self, setup_pre_szn):
        assert isinstance(self.cleaned_sps, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in self.cleaned_sps.columns.tolist()
        assert self.cleaned_sps.iloc[0]["proj_FIP"] <= self.cleaned_sps.iloc[1]["proj_FIP"]
        assert "proj_SVHD" in self.cleaned_rps.columns.tolist()

    def test_clean_pitchers_reg_szn(self, setup_reg_szn):
        assert isinstance(self.cleaned_sps, pd.DataFrame)
        assert "ESPNID", "FANGRAPHSID" in self.cleaned_sps.columns.tolist()
        assert self.cleaned_sps.iloc[0]["proj_FIP"] <= self.cleaned_sps.iloc[1]["proj_FIP"]
        assert "SVHD" in self.cleaned_rps.columns.tolist()
