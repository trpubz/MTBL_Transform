import pandas as pd
import pytest

from app.src.cleaner import Cleaner
from app.src.appraiser import Appraiser
from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS
from app.src.transformer import z_bats, z_arms


class TestAppraiser:
    @pytest.fixture(autouse=True)
    def setup(self):
        combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")
        cleaner = Cleaner(etl_type=ETLType.PRE_SZN, bats=combined_bats, arms=combined_arms)
        cleaned_bats = cleaner.clean_hitters()
        cleaned_sps, cleaned_rps = cleaner.clean_pitchers()
        self.bats = z_bats(cleaned_bats, LG_RULESET, NO_MANAGERS)
        self.arms = z_arms(LG_RULESET, NO_MANAGERS, sps=cleaned_sps, rps=cleaned_rps)
        self.budget_pref = {"bats": 0.65, "sps": 0.20, "rps": .15}
        yield

    def test_instantiation(self):
        app = Appraiser(LG_RULESET, NO_MANAGERS, self.budget_pref, bats=self.bats, arms=self.arms)

        assert isinstance(app.lg_budget, int)
        assert list(app.pos_groups.keys()) == ['DH', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP']
        assert isinstance(app.pos_groups["SS"]["pool_size"], int)

    def test_add_shekels(self):
        app = Appraiser(LG_RULESET, NO_MANAGERS, self.budget_pref, bats=self.bats, arms=self.arms)
        app.add_skekels()

        assert isinstance(app.pos_groups["SS"]["players"].iloc[0].shekels, float)
        assert isinstance(app.pos_groups["SP"]["players"].iloc[0].shekels, float)

