"""
This module contains the Transformer class, which is responsible for calculating z-scores for
batters and pitchers. The z-scores are used to determine the relative value of each player in the
pos group.
Modified: 15 MAY 24
version: 0.1
"""
import numpy as np
import pandas as pd
import math


class Transformer:
    def __init__(self, ruleset: dict, no_managers: int, bats: pd.DataFrame, sps: pd.DataFrame,
                 rps: pd.DataFrame):
        """
        :param ruleset: League specific ruleset
        :param no_managers: Number of managers, establishes the RLP threshold
        :param bats: Dataframe with hitters
        :param sps: Dataframe with pitchers
        :param rps: Dataframe with pitchers
        """
        self.ruleset = ruleset
        self.batting_categories = ruleset["SCORING"]["BATTING"]
        self.pitching_categories = ruleset["SCORING"]["PITCHING"]
        self.no_managers = no_managers
        self.bats = bats
        self.sps = sps
        self.rps = rps

    def z_bats(self) -> dict:
        """
        Z-score for batters group.  RLP is the average of the players right outside the
        draftable set
        :return: dict keyed by the pos
        """
        pos_groups = self.calc_initial_rlp_bats()

        for pos, group in pos_groups.items():
            # first z-score setting is with group sorted on proj_wRC+
            pos_groups[pos]["players"] = self.calculate_z_scores(df=group["players"],
                                                                 rlp_dict=group["rlp"],
                                                                 pos=pos,
                                                                 categories=self.batting_categories)
            if pos != "DH":
                pos_groups["DH"]["players"] = self.cleanup_dh_pos_group(pos_groups=pos_groups,
                                                                        pos=pos)
        # no dups in DH at this point
        pos_groups = self.set_pri_pos(pos_groups)
        assert list(pos_groups.keys())[-1] == "DH"

        for pos, group in pos_groups.items():
            # set new column for primary position
            group["players"]["pri_pos"] = pos
            if pos == "DH":
                # sort on z_total is irrelevant for DH, resort on proj_wRC+
                group["players"].sort_values(by="proj_wRC+", ascending=False, inplace=True)

            rlp_group = self.rlp_group(df=pos_groups[pos]["players"], pos=pos)
            rlp = reduce_rlp_group(rlp_group)
            # second z-score setting is with group sorted on z_total
            pos_groups[pos]["players"] = self.calculate_z_scores(df=group["players"],
                                                                 rlp_dict=rlp,
                                                                 pos=pos,
                                                                 categories=self.batting_categories)

            if pos != "DH":
                # after z-scores are recalculated, set the final RLP group for addition to DH
                rlp_group = self.rlp_group(df=pos_groups[pos]["players"], pos=pos)
                pos_groups["DH"]["players"] = pd.concat([pos_groups["DH"]["players"], rlp_group])
                pos_groups["DH"]["players"] = self.cleanup_dh_pos_group(pos_groups=pos_groups,
                                                                        pos=pos)

        return pos_groups

    def cleanup_dh_pos_group(self, pos_groups: dict, pos: str) -> pd.DataFrame:
        """
        After the position groups are sorted by total_z scores, the top players at each position
        need to be removed from the DH group
        :param pos_groups: all the position groups with players and rlp keys
        :param pos: str for pos to comb through
        :return: Dataframe of DH group
        """
        num_players = self.get_players_at_pos(pos)
        top_players_at_pos = pos_groups[pos]["players"][:num_players]
        dh_players = pos_groups["DH"]["players"]
        no_dups_dh = dh_players[~dh_players["ESPNID"].isin(top_players_at_pos["ESPNID"])]
        no_dups_dh.drop_duplicates(subset="ESPNID", keep="first", inplace=True)

        return no_dups_dh

    def calc_initial_rlp_bats(self, sort_stat: str = "proj_wRC+") -> dict:
        """
        Calculate the Replacement Level Players for each position group the first time through.
        This is required because DH position group has only a few players, and the RLPs from each
        position group are added into the DH group.
        Sorting on this group is done with the sort_stat
        :param: sort_stat: the stat to sort on
        :return: position group dictionary; keys are positions, each position has a pos group and
        RLP dict
        """
        bats = {"DH": {"players": pd.DataFrame(), "rlp": dict}}

        for roster_slot, pos_req in self.ruleset["ROSTER_REQS"]["BATTERS"].items():
            # instantiate the dict
            bats[roster_slot] = {"players": pd.DataFrame(), "rlp": dict} if (
                    roster_slot not in bats) else bats[roster_slot]

            # DH is last position, fill up RLP players into the DH df.
            if roster_slot != "DH":
                bats[roster_slot]["players"] = self.bats[self.bats["positions"].apply(lambda pos:
                                                                                      roster_slot in
                                                                                      pos)]
                rlp_group = self.rlp_group(bats[roster_slot]["players"], roster_slot)
                bats["DH"]["players"] = pd.concat([bats["DH"]["players"], rlp_group])
            else:
                # only one position and it equals DH
                only_dh = self.bats[self.bats["positions"].apply(lambda pos: "DH" in pos[0])]
                bats["DH"]["players"] = pd.concat([bats["DH"]["players"], only_dh])
                # since this is all the RLPs from other positions, resort on proj stat
                bats["DH"]["players"].sort_values(by=sort_stat, ascending=False, inplace=True)
                rlp_group = self.rlp_group(bats["DH"]["players"], roster_slot)

            bats[roster_slot]["rlp"] = reduce_rlp_group(rlp_group)

        # remove from front of dict
        dh = bats.pop("DH")
        # remove any duplicate players
        dh["players"] = dh["players"].drop_duplicates(subset="ESPNID")
        # append to back of dict
        bats["DH"] = dh

        return bats

    def set_pri_pos(self, pos_groups: dict) -> dict:
        """
        Second time through, force player into pri position, remove from alt position group
        :param pos_groups: POS primary keys with players df and rlp dict as secondary key
        :rtype: dict
        :return: pos_groups object type, keyed with pos and pd.DF values

        :discussion: The primary position is actually the lowest index (sorted by z_total) for
        that player's multi-position eligibility.
        For example, if the player is 1B & 3B eligible, and is the 4th best 1B and 5th best 3B,
        ranked by z_total, then the player's primary position is 3B (the lower of the indices).
        The justification for this methodology is that if you can get a player in their lowest
        value group, they can readily be equally or more valuable at their secondary positions.
        This provides the greatest positional flexibility and maximizes the potential return
        value of the player.
        """
        pos_groups = pos_groups.copy()
        pos_list = list(pos_groups.keys())
        for i in range(0, len(pos_list)):
            pos_i = pos_list[i]
            bats = pos_groups[pos_i]["players"]
            for player in bats.itertuples():
                positions = player.positions
                if len(positions) > 1:
                    for alt_pos in positions:
                        if pos_i != alt_pos and alt_pos != "SP":
                            match = pos_groups[alt_pos]["players"][
                                pos_groups[alt_pos]["players"]["ESPNID"] == player.ESPNID]
                            if len(match) == 0:
                                continue
                            # #Index used since itertuples, unwrapping required for direct index
                            current_pos_group_player_idx = math.ceil(
                                player.Index / self.ruleset["ROSTER_REQS"]["BATTERS"][pos_i])
                            match_idx = math.ceil(match.index[0] /
                                                  self.ruleset["ROSTER_REQS"]["BATTERS"][alt_pos])
                            # drop the player from the group for the lower ranking
                            if current_pos_group_player_idx < match_idx:
                                match_index = match.index[0]  # Assuming single match
                                pos_groups[alt_pos]["players"].drop(match_index, inplace=True)
                            else:
                                pos_groups[pos_i]["players"].drop(player.Index, inplace=True)
                            break

        return pos_groups

    def z_arms(self) -> dict:
        """
        Z-score for pitcher group.  RLP is the average of the players right outside the
        draftable set
        :return: dict keyed by the pos
        """
        # ensure #calc_rlp_arms returns the rlp_dict void of the proj_SVHD and proj_QS for SPs
        # and RPs respectively because #calculate_z_scores holds loop logic that will fail if rlp
        # group has these columns and not the players df
        pos_groups = self.calc_rlp_arms(sps=self.sps, rps=self.rps)

        for pos, group in pos_groups.items():
            pos_groups[pos]["players"] = self.calculate_z_scores(df=group["players"],
                                                                 rlp_dict=group["rlp"],
                                                                 pos=pos,
                                                                 categories=self.ruleset["SCORING"][
                                                                     "PITCHING"])

        # re-calculate based on initial sorted
        pos_groups = self.calc_rlp_arms(sps=pos_groups["SP"]["players"],
                                        rps=pos_groups["RP"]["players"])

        for pos, group in pos_groups.items():
            pos_groups[pos]["players"] = self.calculate_z_scores(df=group["players"],
                                                                 rlp_dict=group["rlp"],
                                                                 pos=pos,
                                                                 categories=self.ruleset["SCORING"][
                                                                     "PITCHING"])

        return pos_groups

    def calc_rlp_arms(self, **kwargs) -> dict:
        """
        Calculates RLP groups for all pitchers.
        :return: dict keyed by the pos with two inner keys: players & rlp
        """
        arms = {
            "SP": {"players": kwargs["sps"]},
            "RP": {"players": kwargs["rps"]}
        }

        sp_rlp_group = self.rlp_group(arms["SP"]["players"], "SP")
        # add RLP inner key
        arms["SP"]["rlp"] = reduce_rlp_group(sp_rlp_group)
        # del arms["SP"]["rlp"]["proj_SVHD"]

        rp_rlp_group = self.rlp_group(arms["RP"]["players"], "RP")
        arms["RP"]["rlp"] = reduce_rlp_group(rp_rlp_group)
        # del arms["RP"]["rlp"]["proj_QS"]

        return arms

    def rlp_group(self, df: pd.DataFrame, pos: str) -> pd.DataFrame:
        """
        Calculates RLP groups for single position group
        :param df: Dataframe of position groups
        :param pos: str of the position
        :return: Dataframe of RLP group, just outside position reqs
        """
        num_players = self.get_players_at_pos(pos)
        rlp_range = slice(num_players, num_players + 3)
        return df[rlp_range]

    def get_players_at_pos(self, pos: str) -> int:
        match pos:
            case "SP":
                pos_slots, _ = bucket_wildcard_arms(self.ruleset)
            case "RP":
                _, pos_slots = bucket_wildcard_arms(self.ruleset)
            case _:
                pos_slots = self.ruleset["ROSTER_REQS"]["BATTERS"][pos]

        return self.no_managers * pos_slots

    def calculate_z_scores(self,
                           df: pd.DataFrame,
                           rlp_dict: dict,
                           pos: str,
                           categories: list) -> pd.DataFrame:
        """Calculates z-scores for projected league-relevant stat columns and adds them to the
        DataFrame.
            Also sorts on total z-score

        Note:
            We apply a sqrt normalization on the z-scores to reduce high-end outliers
        Args:
            df (pd.DataFrame): DataFrame containing numeric columns.
            rlp_dict (dict): Dictionary where keys are column names and
                               values are corresponding means.
            pos (str): Position of the players
            categories (list): List of category names

        Returns:
            pd.DataFrame: DataFrame with new z-score columns.
        """
        num_players = self.get_players_at_pos(pos)
        z_df = df.copy().reset_index(drop=True)  # reset index so #loc slicking works in a few lines
        for cat in categories:
            proj_cat = "proj_" + cat
            if proj_cat in rlp_dict:
                rlp_mean = rlp_dict[proj_cat]
                # loc slicing is inclusive, need to subtract 1
                std = z_df.loc[:num_players-1, proj_cat].std(ddof=1)  # Sample standard deviation
                if proj_cat in ["proj_ERA", "proj_WHIP"]:
                    # since lower values are more desirable, need to swap num
                    # sign indicator reapplied after the abs function
                    # #sqrt cannot be applied to neg numbers
                    sign_indicator = np.where(rlp_mean - z_df[proj_cat] >= 0, 1, -1)
                    z_df.loc[:, "z_" + proj_cat] = np.sqrt(
                        np.abs((rlp_mean - z_df[proj_cat]) / std)) * sign_indicator
                else:
                    sign_indicator = np.where(z_df[proj_cat] - rlp_mean >= 0, 1, -1)
                    z_df.loc[:, "z_" + proj_cat] = np.sqrt(
                        np.abs((z_df[proj_cat] - rlp_mean) / std)) * sign_indicator

        drop_cols = ["z_total", "z_swing_miss_percent", "oz_swing_percent"]
        # Calculate Total of 'z' columns
        z_df.loc[:, "z_total"] = z_df.filter(like="z_").drop(drop_cols, axis=1,
                                                             errors="ignore").sum(axis=1)
        z_df.sort_values("z_total", ascending=False, inplace=True)
        z_df.reset_index(drop=True, inplace=True)

        return z_df


def reduce_rlp_group(df: pd.DataFrame) -> dict:
    """
    Reduce by averaging numerical columns
    :param df: Dataframe of position's RLP group
    :return: dict of numerical columns reduced by averaging
    """
    # Select numeric columns
    numeric_cols = df.select_dtypes(include='number').columns.drop(["ESPNID", "MLBID"],
                                                                   errors="ignore")
    return df[numeric_cols].mean().to_dict()


def bucket_wildcard_arms(ruleset: dict) -> tuple:
    """
    Bucket the wildcard pitchers into SPs and RPs based on league ruleset
    :param ruleset: dict
    :return: number of SPs, number of RPs
    """
    wild_card_pitchers = ruleset["ROSTER_REQS"]["PITCHERS"]["P"]
    roster_sps = ruleset["ROSTER_REQS"]["PITCHERS"]["SP"]
    # split the wildcard between SPs and RPs
    wild_card_sps = math.ceil(wild_card_pitchers / 2)
    roster_sps += wild_card_sps
    wild_card_pitchers -= wild_card_sps
    roster_rps = ruleset["ROSTER_REQS"]["PITCHERS"]["RP"] + wild_card_pitchers
    assert roster_sps + roster_rps == (
            ruleset["ROSTER_REQS"]["PITCHERS"]["P"] +
            ruleset["ROSTER_REQS"]["PITCHERS"]["SP"] +
            ruleset["ROSTER_REQS"]["PITCHERS"]["RP"])
    return roster_sps, roster_rps
