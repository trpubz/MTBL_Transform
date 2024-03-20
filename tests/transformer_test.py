import pandas as pd
import pytest

from app.src.cleaner import clean_hitters, clean_pitchers
from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS
from app.src.transformer import *


class TestTransformer:
    @pytest.fixture
    def setup(self):
        self.combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        self.combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")
        yield

    def test_z_arms(self, setup):
        sps, rps = clean_pitchers(self.combined_arms, ETLType.PRE_SZN)
        arms = z_arms(LG_RULESET, NO_MANAGERS, sps=sps, rps=rps)

        assert isinstance(arms["SP"]["players"], pd.DataFrame)
        assert isinstance(arms["RP"]["players"], pd.DataFrame)

    def test_calc_rlp_arms(self, setup):
        sps, rps = clean_pitchers(self.combined_arms, ETLType.PRE_SZN)
        arms = calc_rlp_arms(LG_RULESET, NO_MANAGERS, sps=sps, rps=rps)

        assert isinstance(arms["SP"]["players"], pd.DataFrame)
        assert isinstance(arms["RP"]["players"], pd.DataFrame)
        assert isinstance(arms["SP"]["rlp"], dict)
        assert isinstance(arms["RP"]["rlp"], dict)

    def test_z_bats(self, setup):
        clean_bats = clean_hitters(self.combined_bats, ETLType.PRE_SZN)
        bats = z_bats(clean_bats, LG_RULESET, NO_MANAGERS)

        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert "z_total" in bats["SS"]["players"].columns

    def test_calc_rlp_bats(self, setup):
        clean_bats = clean_hitters(self.combined_bats, ETLType.PRE_SZN)
        # standardize datasets
        bats = calc_initial_rlp_bats(clean_bats, LG_RULESET, NO_MANAGERS)
        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert isinstance(bats["SS"]["rlp"], dict)
