import pandas as pd
import pytest

from app.src.cleaner import Cleaner
from app.src.transformer import Transformer
from app.src.appraiser import Appraiser
from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS


class TestAppraiser:
    @pytest.fixture
    def setup_data(self, request):
        fixture_path = request.param[0]
        etl_type = request.param[1]
        id_cols = ["ESPNID", "FANGRAPHSID", "MLBID"]
        str_dtypes = {col: str for col in id_cols}
        combined_bats = pd.read_json(f"./tests/{fixture_path}/combined_bats.json", dtype=str_dtypes)
        combined_arms = pd.read_json(f"./tests/{fixture_path}/combined_arms.json", dtype=str_dtypes)
        cleaner = Cleaner(etl_type=etl_type, bats=combined_bats, arms=combined_arms)
        cleaned_bats = cleaner.clean_hitters()
        cleaned_sps, cleaned_rps = cleaner.clean_pitchers()
        trxfmr = Transformer(LG_RULESET, NO_MANAGERS, cleaned_bats, cleaned_sps, cleaned_rps)
        self.bats = trxfmr.z_bats()
        self.arms = trxfmr.z_arms()
        self.budget_pref = {
            "bats": {
                "ovr": 0.65,
                "cats": {
                    "HR": 0.20,
                    "R": 0.15,
                    "RBI": 0.10,
                    "SBN": 0.15,
                    "OBP": 0.20,
                    "SLG": 0.20
                }
            },
            "sps": {
                "ovr": 0.20,
                "cats": {
                    "IP": 0.15,
                    "QS": 0.20,
                    "ERA": 0.20,
                    "WHIP": 0.20,
                    "K/9": 0.25
                }
            },
            "rps": {
                "ovr": 0.15,
                "cats": {
                    "IP": 0.15,
                    "SVHD": 0.20,
                    "ERA": 0.20,
                    "WHIP": 0.20,
                    "K/9": 0.25
                }
            }
        }
        self.app = Appraiser(LG_RULESET, NO_MANAGERS, self.budget_pref, bats=self.bats,
                             arms=self.arms)
        yield

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_instantiation_reg_szn(self, setup_data):
        assert isinstance(self.app.lg_budget, int)
        assert set(self.app.pos_groups.keys()) == {
            'DH', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP'}
        assert isinstance(self.app.pos_groups["SS"]["pool_size"], int)

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_calc_lg_bat_cat_tot_reg_szn(self, setup_data):
        # tests for calculate_league_category_totals
        self.app.calculate_league_batting_category_totals()
        assert set(self.app.lg_category_totals.keys()) == {"BATTING"}
        assert set(self.app.lg_category_totals["BATTING"]["TOTALS"].keys()) == {"HR", "R", "RBI",
                                                                                "SBN", "OBP", "SLG"}
        # tests for batting pos_group_category_totals
        assert set(self.app.lg_category_totals["BATTING"]["SS"].keys()) == {"HR", "R", "RBI", "SBN",
                                                                            "OBP", "SLG"}
        # assert set(self.app.lg_category_totals["PITCHING"]["SP"].keys()) == {"IP", "QS", "ERA",
        #                                                                      "WHIP", "K/9"}
        # assert set(self.app.lg_category_totals["PITCHING"]["RP"].keys()) == {"IP", "SVHD", "ERA",
        #                                                                      "WHIP", "K/9"}

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_calc_bat_cat_weights_reg_szn(self, setup_data):
        self.app.calculate_league_batting_category_totals()
        self.app.calculate_batting_category_weights_shekels()

        assert isinstance(self.app.lg_category_totals["BATTING"]["SS"]["HR_shekel_per_z"], float)
        tot_bat_shekels = 0
        # pop off the totals key so only pos keys remain
        self.app.lg_category_totals["BATTING"].pop("TOTALS")
        for pos, cat_shekels in self.app.lg_category_totals["BATTING"].items():
            num_players = self.app.pos_groups[pos]["pool_size"]
            for cat, value in cat_shekels.items():
                # only grab category shekels per pos group
                if "shekel_per_z" in cat:
                    tot_bat_shekels += value * sum(self.app.pos_groups[pos]["players"].loc[
                                                   :num_players - 1, f"z_proj_{cat.split('_')[0]}"])

        assert tot_bat_shekels.__round__(0) == round(
            self.app.lg_budget * self.budget_pref["bats"]["ovr"], 0)

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_calc_pit_cat_weights_reg_szn(self, setup_data):
        self.app.calculate_pitching_category_weights_shekels()

        assert isinstance(self.app.lg_category_totals["SP"]["IP_shekel_per_z"], float)
        assert isinstance(self.app.lg_category_totals["RP"]["IP_shekel_per_z"], float)
        assert set(self.app.lg_category_totals["SP"].keys()) == {"IP_shekel_per_z",
                                                                 "QS_shekel_per_z",
                                                                 "ERA_shekel_per_z",
                                                                 "WHIP_shekel_per_z",
                                                                 "K/9_shekel_per_z"}
        assert set(self.app.lg_category_totals["RP"].keys()) == {"IP_shekel_per_z",
                                                                 "SVHD_shekel_per_z",
                                                                 "ERA_shekel_per_z",
                                                                 "WHIP_shekel_per_z",
                                                                 "K/9_shekel_per_z"}
        tot_sp_shekels = 0
        num_players = self.app.pos_groups["SP"]["pool_size"]
        for cat, value in self.app.lg_category_totals["SP"].items():
            if "shekel_per_z" in cat:
                tot_sp_shekels += value * sum(self.app.pos_groups["SP"]["players"].loc[
                                              :num_players - 1, f"z_proj_{cat.split('_')[0]}"])

        assert tot_sp_shekels.__round__(0) == round(
            self.app.lg_budget * self.budget_pref["sps"]["ovr"], 0)

        tot_rp_shekels = 0
        num_players = self.app.pos_groups["RP"]["pool_size"]
        for cat, value in self.app.lg_category_totals["RP"].items():
            if "shekel_per_z" in cat:
                tot_rp_shekels += value * sum(self.app.pos_groups["RP"]["players"].loc[
                                              :num_players - 1, f"z_proj_{cat.split('_')[0]}"])

        assert tot_rp_shekels.__round__(0) == round(
            self.app.lg_budget * self.budget_pref["rps"]["ovr"], 0)

    @pytest.mark.parametrize("setup_data", [
        ("fixtures", ETLType.PRE_SZN)], indirect=True)
    def test_instantiation_pre_szn(self, setup_data):
        assert isinstance(self.app.lg_budget, int)
        assert set(self.app.pos_groups.keys()) == {'DH', 'C', '1B', '2B', '3B', 'SS', 'OF',
                                                   'SP', 'RP'}
        assert isinstance(self.app.pos_groups["SS"]["pool_size"], int)

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_add_shekels_reg_szn(self, setup_data):
        self.app.calculate_league_batting_category_totals()
        self.app.calculate_batting_category_weights_shekels()
        self.app.calculate_pitching_category_weights_shekels()
        self.app.add_skekels()

        for pos, pos_group in self.app.pos_groups.items():
            assert isinstance(pos_group["players"].loc[0, "shekels"], float)
