import pytest

from app.src.cleaner import Cleaner
from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS
from app.src.transformer import *


class TestTransformer:
    @pytest.fixture
    def setup_data(self, request):
        fixture_path = request.param[0]
        etl_type = request.param[1]
        id_cols = ["ESPNID", "FANGRAPHSID", "MLBID"]
        str_dtypes = {col: str for col in id_cols}
        combined_bats = pd.read_json(
            f"./tests/{fixture_path}/combined_bats.json", dtype=str_dtypes)
        combined_arms = pd.read_json(
            f"./tests/{fixture_path}/combined_arms.json", dtype=str_dtypes)
        cleaner = Cleaner(etl_type=etl_type, bats=combined_bats, arms=combined_arms)
        self.cleaned_bats = cleaner.clean_hitters()
        self.cleaned_sps, self.cleaned_rps = cleaner.clean_pitchers()
        self.transformer = Transformer(ruleset=LG_RULESET,
                                       no_managers=NO_MANAGERS,
                                       bats=self.cleaned_bats,
                                       sps=self.cleaned_sps,
                                       rps=self.cleaned_rps)
        yield

    @pytest.fixture
    def setup_str_dtypes(self):
        id_cols = ["ESPNID", "FANGRAPHSID", "MLBID"]
        str_dtypes = {col: str for col in id_cols}
        return str_dtypes

    @pytest.fixture
    def setup_pre_szn(self, setup_str_dtypes):
        str_dtypes = setup_str_dtypes
        combined_bats = pd.read_json(
            "./tests/fixtures/combined_bats.json", dtype=str_dtypes)
        combined_arms = pd.read_json(
            "./tests/fixtures/combined_arms.json", dtype=str_dtypes)
        cleaner = Cleaner(etl_type=ETLType.PRE_SZN, bats=combined_bats, arms=combined_arms)
        self.cleaned_bats = cleaner.clean_hitters()
        self.cleaned_sps, self.cleaned_rps = cleaner.clean_pitchers()
        self.transformer = Transformer(ruleset=LG_RULESET,
                                       no_managers=NO_MANAGERS,
                                       bats=self.cleaned_bats,
                                       sps=self.cleaned_sps,
                                       rps=self.cleaned_rps)
        yield

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_z_bats_reg_szn(self, setup_data):
        bats = self.transformer.z_bats()

        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert "z_total" in bats["SS"]["players"].columns
        assert bats["SS"]["players"].loc[0, "z_total"] >= bats["SS"]["players"].loc[1, "z_total"]
        assert bats["DH"]["players"].loc[0, "z_total"] >= bats["DH"]["players"].loc[1, "z_total"]
        assert bats["DH"]["players"].duplicated("ESPNID").any() == False

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_z_bats_pre_szn(self, setup_data):
        bats = self.transformer.z_bats()

        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert "z_total" in bats["SS"]["players"].columns

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_calc_initial_rlp_bats_reg_szn(self, setup_data):
        bats = self.transformer.calc_initial_rlp_bats()

        assert ["C", "1B", "2B", "3B", "SS", "OF", "DH"] == list(bats.keys())
        assert isinstance(bats["SS"]["players"], pd.DataFrame)
        assert isinstance(bats["SS"]["rlp"], dict)

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_calc_z_score_bats_reg_szn(self, setup_data):
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

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_cleanup_dh_pos_group_reg_szn(self, setup_data):
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

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_set_pri_pos_bats_reg_szn(self, setup_data):
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

    def test_z_arms_pre_szn(self, setup_pre_szn):
        arms = self.transformer.z_arms()

        assert isinstance(arms["SP"]["players"], pd.DataFrame)
        assert isinstance(arms["RP"]["players"], pd.DataFrame)

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_z_arms_reg_szn(self, setup_data):
        arms = self.transformer.z_arms()
        # TODO: assert the number of SPs and RPs are correct with wild card pitchers figured out

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_calc_z_scores_arms_reg_szn(self, setup_data):
        pos_groups = self.transformer.calc_rlp_arms(sps=self.cleaned_sps,
                                                    rps=self.cleaned_rps)

        for pos, group in pos_groups.items():
            pos_groups[pos]["players"] = self.transformer.calculate_z_scores(
                df=group["players"],
                rlp_dict=group["rlp"],
                pos=pos,
                categories=self.transformer.pitching_categories)

        sp_players = pos_groups["SP"]["players"]
        rp_players = pos_groups["RP"]["players"]
        assert isinstance(sp_players, pd.DataFrame)
        assert isinstance(rp_players, pd.DataFrame)
        assert {"z_total", "proj_QS"}.issubset(set(sp_players.columns))
        assert "proj_SVHD" not in sp_players.columns
        assert {"z_total", "proj_SVHD"}.issubset(set(rp_players.columns))
        assert "proj_QS" not in rp_players.columns
        assert sp_players.loc[0, "z_total"] >= sp_players.loc[1, "z_total"]
        assert rp_players.loc[0, "z_total"] >= rp_players.loc[1, "z_total"]

    @pytest.mark.parametrize("setup_data", [
        ("fixtures_reg_szn", ETLType.REG_SZN)], indirect=True)
    def test_calc_rlp_arms_reg_szn(self, setup_data):
        arms = self.transformer.calc_rlp_arms(sps=self.cleaned_sps,
                                              rps=self.cleaned_rps)

        assert isinstance(arms["SP"]["players"], pd.DataFrame)
        assert "proj_SVHD" not in arms["SP"]["players"].columns
        assert isinstance(arms["RP"]["players"], pd.DataFrame)
        assert "proj_QS" not in arms["RP"]["players"].columns
        assert isinstance(arms["SP"]["rlp"], dict)
        assert "proj_SVHD" not in arms["SP"]["rlp"].keys()
        assert isinstance(arms["RP"]["rlp"], dict)
        assert "proj_QS" not in arms["RP"]["rlp"].keys()

    def test_calc_rlp_arms_pre_szn(self, setup_pre_szn):
        arms = self.transformer.calc_rlp_arms(sps=self.cleaned_sps,
                                              rps=self.cleaned_rps)

        assert isinstance(arms["SP"]["players"], pd.DataFrame)
        assert isinstance(arms["RP"]["players"], pd.DataFrame)
        assert isinstance(arms["SP"]["rlp"], dict)
        assert isinstance(arms["RP"]["rlp"], dict)
