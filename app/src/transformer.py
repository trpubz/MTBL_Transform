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
    pos_groups = calc_initial_rlp_bats(df, ruleset, no_managers)

    for pos, group in pos_groups.items():
        pos_groups[pos]["players"] = calculate_zscores(
            pos, group["players"], group["rlp"], ruleset["SCORING"]["BATTING"])

    # second time through, force player into pri position, remove from alt position group
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
                        if len(match) == 0: continue
                        # #Index used since itertuples, unwrapping required for direct index
                        p_idx = math.ceil(player.Index / ruleset["ROSTER_REQS"]["BATTERS"][pos_i])
                        m_idx = math.ceil(match.index[0] /
                                          ruleset["ROSTER_REQS"]["BATTERS"][alt_pos])
                        if p_idx < m_idx:
                            match_index = match.index[0]  # Assuming single match
                            pos_groups[alt_pos]["players"].drop(match_index, inplace=True)
                        else:
                            pos_groups[pos_i]["players"].drop(player.Index, inplace=True)

                        break

    for pos, group in pos_groups.items():
        rlp = rlp_group(pos_groups[pos]["players"], ruleset["ROSTER_REQS"]["BATTERS"][pos],
                        no_managers)
        rlp = reduce_rlp_group(rlp)
        pos_groups[pos]["players"] = calculate_zscores(
            pos, group["players"], rlp, ruleset["SCORING"]["BATTING"])

    return pos_groups


def z_arms(ruleset: dict, no_managers: int, **kwargs) -> dict:
    """
    Z-score for pitcher group.  RLP is the average of the players right outside the draftable set
    :param kwargs: keyed dataframes with pitchers
    :param ruleset: League specific ruleset
    :param no_managers: Number of managers, establishes the RLP threshold
    :return: dict keyed by the pos
    """
    pos_groups = calc_rlp_arms(ruleset, no_managers, **kwargs)

    for pos, group in pos_groups.items():
        pos_groups[pos]["players"] = calculate_zscores(
            pos, group["players"], group["rlp"], ruleset["SCORING"]["PITCHING"])

    # re-calculate based on initial sorted
    pos_groups = calc_rlp_arms(ruleset, no_managers,
                               sps=pos_groups["SP"]["players"], rps=pos_groups["RP"]["players"])

    for pos, group in pos_groups.items():
        pos_groups[pos]["players"] = calculate_zscores(
            pos, group["players"], group["rlp"], ruleset["SCORING"]["PITCHING"])

    return pos_groups


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
            if cat in ["ERA", "WHIP"]:  # since lower values are more desirable, need to swap num
                # sign indicator reapplied after the abs function
                # #sqrt cannot be applied to neg numbers
                sign_indicator = np.where(rlp_mean - df[cat] >= 0, 1, -1)
                df["z" + cat] = np.sqrt(np.abs((rlp_mean - df[cat]) / std)) * sign_indicator
            else:
                sign_indicator = np.where(df[cat] - rlp_mean >= 0, 1, -1)
                df["z" + cat] = np.sqrt(np.abs((df[cat] - rlp_mean) / std)) * sign_indicator

    drop_cols = ["z_total", "z_swing_miss_percent", "oz_swing_percent"]
    # Calculate Total of 'z' columns
    df["z_total"] = df.filter(like="z").drop(drop_cols, axis=1, errors="ignore").sum(axis=1)
    df.sort_values("z_total", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def calc_rlp_arms(ruleset: dict, no_managers: int, **kwargs) -> dict:
    arms = {"SP": {"players": kwargs["sps"]},
            "RP": {"players": kwargs["rps"]}}

    wild_card_ps = ruleset["ROSTER_REQS"]["PITCHERS"]["P"]
    ros_sps = ruleset["ROSTER_REQS"]["PITCHERS"]["SP"]
    wild_card_sps = math.ceil(wild_card_ps / 2)
    ros_sps += wild_card_sps
    wild_card_ps -= wild_card_sps
    ros_rps = ruleset["ROSTER_REQS"]["PITCHERS"]["RP"] + wild_card_ps

    group = rlp_group(arms["SP"]["players"], ros_sps, no_managers)
    arms["SP"]["rlp"] = reduce_rlp_group(group)

    group = rlp_group(arms["RP"]["players"], ros_rps, no_managers)
    arms["RP"]["rlp"] = reduce_rlp_group(group)

    return arms


def calc_initial_rlp_bats(df: any, ruleset: dict, no_managers: int) -> dict:
    """
    Calculate the Replacement Level Players for each position group
    :param df: Dataframe with hitters
    :param no_managers: League specific ruleset
    :param ruleset: Number of managers, establishes the RLP threshold
    :return: position group dictionary; keys are positions, each position has a pos group and RLP
    dict
    """
    bats = {"DH": {"players": pd.DataFrame(), "rlp": None}}

    for roster_slot, num in ruleset["ROSTER_REQS"]["BATTERS"].items():
        # instantiate the dict
        bats[roster_slot] = {"players": None, "rlp": None} if (
                roster_slot not in bats) else bats[roster_slot]

        # DH is last position, fill up RLP players into the DH df.
        if roster_slot != "DH":
            bats[roster_slot]["players"] = df[df["positions"].apply(lambda pos: roster_slot in pos)]
            rlp = rlp_group(bats[roster_slot]["players"], num, no_managers)
            bats["DH"]["players"] = pd.concat([bats["DH"]["players"], rlp])
        else:
            # only one position and it equals DH
            only_dh = df[df["positions"].apply(lambda pos: "DH" in pos[0])]
            bats["DH"]["players"] = pd.concat([bats["DH"]["players"], only_dh])
            # since this is all the RLPs from other positions, resort on wRAA
            bats["DH"]["players"].sort_values(by="wRAA", ascending=False, inplace=True)
            rlp = rlp_group(bats["DH"]["players"], num, no_managers)

        bats[roster_slot]["rlp"] = reduce_rlp_group(rlp)
    return bats


def rlp_group(df: pd.DataFrame, ros_req: int, no_managers: int) -> pd.DataFrame:
    rlp_range = slice(no_managers * ros_req, no_managers * ros_req + 3)
    return df[rlp_range]


def reduce_rlp_group(df: pd.DataFrame) -> dict:
    # Select numeric columns
    numeric_cols = df.select_dtypes(include='number').columns.drop(["ESPNID", "MLBID"], errors="ignore")
    return df[numeric_cols].mean().to_dict()
