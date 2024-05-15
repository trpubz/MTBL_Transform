import pytest

from app.src.cleaner import Cleaner
from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS
from app.src.transformer import *


class TestTransformer:
    @pytest.fixture
    def setup_pre_szn(self):
        combined_bats = pd.read_json("./tests/fixtures/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures/combined_arms.json")
        cleaner = Cleaner(etl_type=ETLType.PRE_SZN, bats=combined_bats, arms=combined_arms)
        self.cleaned_bats = cleaner.clean_hitters()
        self.cleaned_sps, self.cleaned_rps = cleaner.clean_pitchers()
        self.transformer = Transformer(ruleset=LG_RULESET,
                                       no_managers=NO_MANAGERS,
                                       bats=self.cleaned_bats,
                                       sps=self.cleaned_sps,
                                       rps=self.cleaned_rps)
        yield

    @pytest.fixture
    def setup_reg_szn(self):
        combined_bats = pd.read_json("./tests/fixtures_reg_szn/combined_bats.json")
        combined_arms = pd.read_json("./tests/fixtures_reg_szn/combined_arms.json")
        cleaner = Cleaner(etl_type=ETLType.REG_SZN, bats=combined_bats, arms=combined_arms)
        self.cleaned_bats = cleaner.clean_hitters()
        self.cleaned_sps, self.cleaned_rps = cleaner.clean_pitchers()
        self.transformer = Transformer(ruleset=LG_RULESET,
                                       no_managers=NO_MANAGERS,
                                       bats=self.cleaned_bats,
                                       sps=self.cleaned_sps,
                                       rps=self.cleaned_rps)
        yield

    def test_z_bats_reg_szn(self, setup_reg_szn):
        bats = self.transformer.z_bats()

        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert "z_total" in bats["SS"]["players"].columns
        assert bats["SS"]["players"].loc[0, "z_total"] >= bats["SS"]["players"].loc[1, "z_total"]
        assert bats["DH"]["players"].loc[0, "z_total"] >= bats["DH"]["players"].loc[1, "z_total"]
        assert bats["DH"]["players"].duplicated("ESPNID").any() == False

    def test_calc_initial_rlp_bats_reg_szn(self, setup_reg_szn):
        bats = self.transformer.calc_initial_rlp_bats()

        assert ["C", "1B", "2B", "3B", "SS", "OF", "DH"] == list(bats.keys())
        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert isinstance(bats["SS"]["rlp"], dict)

    def test_calc_z_score_bats_reg_szn(self, setup_reg_szn):
        pos_groups = self.transformer.calc_initial_rlp_bats()

        for pos, group in pos_groups.items():
            pos_groups[pos]["players"] = self.transformer.calculate_z_scores(
                df=group["players"],
                rlp_dict=group["rlp"],
                pos=pos,
                categories=self.transformer.batting_categories)

        ss_players = pos_groups["SS"]["players"]
        assert isinstance(ss_players, pd.DataFrame)
        assert "z_total" in ss_players.columns
        assert ss_players.loc[0, "z_total"] >= ss_players.loc[1, "z_total"]

    def test_cleanup_dh_pos_group_reg_szn(self, setup_reg_szn):
        pos_groups = self.transformer.calc_initial_rlp_bats()
        for pos, group in pos_groups.items():
            pos_groups[pos]["players"] = self.transformer.calculate_z_scores(
                df=group["players"],
                rlp_dict=group["rlp"],
                pos=pos,
                categories=self.transformer.batting_categories)
            if pos != "DH":
                pos_groups["DH"]["players"] = self.transformer.cleanup_dh_pos_group(
                    pos_groups=pos_groups,
                    pos=pos)

                ros_reqs = self.transformer.ruleset["ROSTER_REQS"]["BATTERS"][pos]
                num_players = ros_reqs * self.transformer.no_managers

                assert True not in pos_groups["DH"]["players"]["ESPNID"].isin(
                    pos_groups[pos]["players"][:num_players]["ESPNID"]).to_list()

    def test_set_pri_pos_bats_reg_szn(self, setup_reg_szn):
        """
        Expect the player to end up in the lower indexed position group.
        Player 5 is rank 1 in the 1B group and rank 5 in the OF group.  Thus, he should be
        pri-pos for OF group.
        """
        pos_groups = {
            "1B": {"players":
                       pd.DataFrame({
                           "ESPNID": [1, 2, 3, 4, 5],
                           "proj_wRC+": [100, 110, 120, 130, 140],
                           "z_total": [1, 2, 3, 4, 5],
                           "positions": [["1B"], ["1B"], ["1B"], ["1B"], ["1B", "OF"]]
                       }).sort_values("z_total", ascending=False)},
            "OF": {"players":
                       pd.DataFrame({
                           "ESPNID": [5, 7, 8, 9, 10],
                           "proj_wRC+": [100, 110, 120, 130, 140],
                           "z_total": [1, 2, 3, 4, 5],
                           "positions": [["1B", "OF"], ["OF"], ["OF"], ["OF"], ["OF"]]
                       }).sort_values("z_total", ascending=False)},
        }

        pos_groups = self.transformer.set_pri_pos(pos_groups)

        assert 5 not in pos_groups["1B"]["players"]["ESPNID"].to_list()

    def test_calc_rlp_bats_pre_szn(self, setup_pre_szn):
        # standardize datasets
        bats = self.transformer.calc_initial_rlp_bats()
        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert isinstance(bats["SS"]["rlp"], dict)

    def test_z_arms(self, setup_pre_szn):
        arms = self.transformer.z_arms()

        assert isinstance(arms["SP"]["players"], pd.DataFrame)
        assert isinstance(arms["RP"]["players"], pd.DataFrame)

    def test_calc_rlp_arms(self, setup_pre_szn):
        arms = self.transformer.calc_rlp_arms(sps=self.cleaned_sps,
                                              rps=self.cleaned_rps)

        assert isinstance(arms["SP"]["players"], pd.DataFrame)
        assert isinstance(arms["RP"]["players"], pd.DataFrame)
        assert isinstance(arms["SP"]["rlp"], dict)
        assert isinstance(arms["RP"]["rlp"], dict)

    def test_z_bats_pre_szn(self, setup_pre_szn):
        bats = self.transformer.z_bats()

        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert "z_total" in bats["SS"]["players"].columns
