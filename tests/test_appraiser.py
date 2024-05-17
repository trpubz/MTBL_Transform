import pandas as pd
import pytest

from app.src.cleaner import Cleaner
from app.src.transformer import Transformer
from app.src.appraiser import Appraiser
from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS


class TestAppraiser:
    @pytest.fixture
    def setup_pre_szn(self):
        combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")
        self.cleaner = Cleaner(etl_type=ETLType.PRE_SZN, bats=combined_bats, arms=combined_arms)
        yield

    @pytest.fixture
    def setup_reg_szn(self):
        combined_bats = pd.read_json("./tests/fixtures_reg_szn/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures_reg_szn/combined_arms.json")
        cleaner = Cleaner(etl_type=ETLType.REG_SZN, bats=combined_bats, arms=combined_arms)
        cleaned_bats = cleaner.clean_hitters()
        cleaned_sps, cleaned_rps = cleaner.clean_pitchers()
        trxfmr = Transformer(LG_RULESET, NO_MANAGERS, cleaned_bats, cleaned_sps, cleaned_rps)
        self.bats = trxfmr.z_bats()
        self.arms = trxfmr.z_arms()
        self.budget_pref = {"bats": 0.65, "sps": 0.20, "rps": .15}
        self.app = Appraiser(LG_RULESET, NO_MANAGERS, self.budget_pref, bats=self.bats,
                             arms=self.arms)
        yield

    # def setup(self, cleaner: Cleaner):
    #     cleaned_bats = cleaner.clean_hitters()
    #     cleaned_sps, cleaned_rps = cleaner.clean_pitchers()
    #     trxfmr = Transformer(LG_RULESET, NO_MANAGERS, cleaned_bats, cleaned_sps, cleaned_rps)
    #     self.bats = trxfmr.z_bats()
    #     self.arms = trxfmr.z_arms()
    #     self.budget_pref = {"bats": 0.65, "sps": 0.20, "rps": .15}
    #     self.app = Appraiser(LG_RULESET, NO_MANAGERS, self.budget_pref, bats=self.bats,
    #                          arms=self.arms)

    def test_instantiation_reg_szn(self, setup_reg_szn):
        assert isinstance(self.app.lg_budget, int)
        assert list(self.app.pos_groups.keys()) == ['DH', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP']
        assert isinstance(self.app.pos_groups["SS"]["pool_size"], int)
        assert self.app.lg_category_totals.keys() == ["BATTERS", "PITCHERS"]

    def test_instantiation_pre_szn(self, setup_pre_szn):
        assert isinstance(self.app.lg_budget, int)
        assert list(self.app.pos_groups.keys()) == ['DH', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP']
        assert isinstance(self.app.pos_groups["SS"]["pool_size"], int)

    def test_add_shekels_pre_szn(self, setup_pre_szn):
        self.app.add_skekels()

        assert isinstance(self.app.pos_groups["SS"]["players"].iloc[0].shekels, float)
        assert isinstance(self.app.pos_groups["SP"]["players"].iloc[0].shekels, float)
