import pandas as pd

from app.src.mtbl_globals import LG_RULESET


def z_bats(df: pd.DataFrame, ruleset: LG_RULESET, no_managers: int) -> pd.DataFrame:
    """
    Z-score for batters group.  RLP is the average of the players right outside the draftable set.
    Need to segment position groups.
    :param df: Dataframe with hitters
    :param ruleset: League specific ruleset
    :param no_managers: Number of managers, establishes the RLP threshold
    :return:
    """
    bats = None
    rlp_range = slice(no_managers, no_managers + 3)
    rlp_df = df.loc[rlp_range]

    return bats
