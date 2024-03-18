import numpy as np
import pandas as pd
import math


def z_bats(df: pd.DataFrame, ruleset: dict, no_managers: int) -> dict:
    """
    Z-score for batters group.  RLP is the average of the players right outside the draftable set
    :param df: Dataframe with hitters
    :param ruleset: League specific ruleset
    :param no_managers: Number of managers, establishes the RLP threshold
    :return: dict keyed by the pos
    """
    pos_groups = calc_initial_rlp(df, ruleset, no_managers)

    for pos, group in pos_groups.items():
        pos_groups[pos]["bats"] = calculate_zscores(
            pos, group["bats"], group["rlp"], ruleset["SCORING"]["BATTING"])

    # second time through, force player into pri position, remove from alt position group
    pos_list = list(pos_groups.keys())
    for i in range(0, len(pos_list)):
        pos_i = pos_list[i]
        bats = pos_groups[pos_i]["bats"]
        for player in bats.itertuples():
            positions = player.positions
            if len(positions) > 1:
                for alt_pos in positions:
                    if pos_i != alt_pos and alt_pos != "SP":
                        match = pos_groups[alt_pos]["bats"][
                            pos_groups[alt_pos]["bats"]["ESPNID"] == player.ESPNID]
                        if len(match) == 0: continue
                        # #Index used since itertuples, unwrapping required for direct index
                        p_idx = math.ceil(player.Index / ruleset["ROSTER_REQS"]["BATTERS"][pos_i])
                        m_idx = math.ceil(match.index[0] /
                                          ruleset["ROSTER_REQS"]["BATTERS"][alt_pos])
                        if p_idx < m_idx:
                            match_index = match.index[0]  # Assuming single match
                            pos_groups[alt_pos]["bats"].drop(match_index, inplace=True)
                        else:
                            pos_groups[pos_i]["bats"].drop(player.Index, inplace=True)

                        break

    for pos, group in pos_groups.items():
        rlp = rlp_group(pos_groups[pos]["bats"], ruleset["ROSTER_REQS"]["BATTERS"][pos], no_managers)
        rlp = reduce_rlp_group(rlp)
        pos_groups[pos]["bats"] = calculate_zscores(
            pos, group["bats"], rlp, ruleset["SCORING"]["BATTING"])

    return pos_groups


def z_arms(ruleset: dict, no_managers: int, **kwargs) -> dict:
    """
    Z-score for pitcher group.  RLP is the average of the players right outside the draftable set
    :param kwargs: keyed dataframes with pitchers
    :param ruleset: League specific ruleset
    :param no_managers: Number of managers, establishes the RLP threshold
    :return: dict keyed by the pos
    """
    pos_groups = calc_initial_rlp(df, ruleset, no_managers)


def calculate_zscores(pos, df, rlp_dict, categories: list):
    """Calculates z-scores for numeric columns and adds them to the DataFrame.

    Note:
        We apply a sqrt normalization on the z-scores to reduce highend outliers
    Args:
        pos (string): position of the player group
        df (pd.DataFrame): DataFrame containing numeric columns.
        rlp_dict (dict): Dictionary where keys are column names and
                           values are corresponding means.
        categories (list): List of category names

    Returns:
        pd.DataFrame: DataFrame with new z-score columns.
    """
    # df.drop(columns=categories, inplace=True)

    for cat in categories:
        if cat in rlp_dict:
            rlp_mean = rlp_dict[cat]
            std = df[cat].std(ddof=1)  # Sample standard deviation
            df["z" + cat] = np.sqrt((df[cat] - rlp_mean) / std)

    # Calculate Total of 'z' columns
    df["z_total"] = df.filter(like="z").drop("z_total", axis=1, errors="ignore").sum(axis=1)
    df.sort_values("z_total", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def calc_initial_rlp(df: any, ruleset: dict, no_managers: int) -> dict:
    """
    Calculate the Replacement Level Players for each position group
    :param df: Dataframe with hitters
    :param no_managers: League specific ruleset
    :param ruleset: Number of managers, establishes the RLP threshold
    :return: position group dictionary; keys are positions, each position has a pos group and RLP
    dict
    """
    bats = {"DH": {"bats": pd.DataFrame(), "rlp": None}}

    for roster_slot, num in ruleset["ROSTER_REQS"]["BATTERS"].items():
        # instantiate the dict
        bats[roster_slot] = {"bats": None, "rlp": None} if (
                roster_slot not in bats) else bats[roster_slot]

        # DH is last position, fill up RLP players into the DH df.
        if roster_slot != "DH":
            bats[roster_slot]["bats"] = df[df["positions"].apply(lambda pos: roster_slot in pos)]
            rlp = rlp_group(bats[roster_slot]["bats"], num, no_managers)
            bats["DH"]["bats"] = pd.concat([bats["DH"]["bats"], rlp])
        else:
            # only one position and it equals DH
            only_dh = df[df["positions"].apply(lambda pos: "DH" in pos[0])]
            bats["DH"]["bats"] = pd.concat([bats["DH"]["bats"], only_dh])
            # since this is all the RLPs from other positions, resort on wRAA
            bats["DH"]["bats"].sort_values(by="wRAA", ascending=False, inplace=True)
            rlp = rlp_group(bats["DH"]["bats"], num, no_managers)

        bats[roster_slot]["rlp"] = reduce_rlp_group(rlp)
    return bats


def rlp_group(df: pd.DataFrame, ros_req: int, no_managers: int) -> pd.DataFrame:
    rlp_range = slice(no_managers * ros_req, no_managers * ros_req + 3)
    return df[rlp_range]


def reduce_rlp_group(df: pd.DataFrame) -> dict:
    # Select numeric columns
    numeric_cols = df.select_dtypes(include='number').columns.drop(["ESPNID", "MLBID"])
    return df[numeric_cols].mean().to_dict()
