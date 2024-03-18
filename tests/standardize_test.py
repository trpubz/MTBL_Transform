import pandas as pd
import pytest

from app.src import ETLType
from app.src.cleaner import clean_hitters
from app.src.mtbl_globals import LG_RULESET, NO_MANAGERS
from app.src.standardize import z_bats


class TestStandardize:
    @pytest.fixture
    def setup(self):
        self.combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        self.combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")
        yield

    def test_z_bats(self, setup):
        clean_bats = clean_hitters(self.combined_bats, ETLType.PRE_SZN)
        # standardize datasets
        bats = z_bats(clean_bats, LG_RULESET, NO_MANAGERS)
        assert isinstance(bats, pd.DataFrame)
