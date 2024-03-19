from unittest import case

import pandas as pd

from app.src.mtbl_globals import ETLType


def clean_hitters(df: pd.DataFrame, etl_type: ETLType) -> pd.DataFrame:
    """
    Clean hitters, remove unnecessary columns, sort, and get data ready for standardization
    :param df: dataframe containing combined bats
    :param etl_type: PRESZN or REGSZN
    :return: cleaned bats dataframe
    """
    clean_bats = None
    columns = []
    match etl_type:
        case ETLType.PRE_SZN:
            # lowercase name is used since that is ESPN's style
            columns = ['ESPNID', 'FANGRAPHSID', 'name', 'Team', 'positions', 'G', 'PA', 'AB', 'H',
                       'HR', 'R', 'RBI', 'SB', 'CS', 'SBN', 'AVG', 'OBP', 'SLG', 'OPS', 'BB%',
                       'K%', 'wOBA', 'ISO', 'BABIP', 'wRC', 'wRAA', 'wRC+', 'WAR', 'MLBID',
                       'attempts', 'avg_hit_angle', 'anglesweetspotpercent', 'max_hit_speed',
                       'avg_hit_speed', 'ev50', 'fbld', 'gb', 'max_distance', 'avg_distance',
                       'avg_hr_distance', 'ev95plus', 'ev95percent', 'barrels', 'brl_percent',
                       'brl_pa']
        # case ETLType.REG_SZN:
        #     pass

    df["SBN"] = df["SB"] - df["CS"]
    clean_bats = df[columns].sort_values(by="wRAA", ascending=False)

    return clean_bats


def clean_pitchers(df: pd.DataFrame, etl_type: ETLType) -> (pd.DataFrame, pd.DataFrame):
    """
    Clean pitchers, remove unnecessary columns, sort, and get data ready for standardization
    :param df: dataframe containing combined arms
    :param etl_type: PRESZN or REGSZN
    :return: tuple of dataframes containing SPs and RPs
    """
    df["SVHD"] = df["SV"] + df["HLD"]
    clean_sps = df[df["QS"] > df["SVHD"]]
    clean_rps = df[df["SVHD"] >= df["QS"]]
    columns = []
    match etl_type:
        case ETLType.PRE_SZN:
            # lowercase name is used since that is ESPN's style
            columns = ['ESPNID', 'FANGRAPHSID', 'name', 'Team', 'positions', 'G', 'GS', 'IP',
                       'QS', 'SV', 'HLD', 'SVHD', 'ERA', 'WHIP', 'K/9', 'FIP', 'BB/9', 'K/BB',
                       'HR/9', 'BABIP', 'WAR', 'MLBID', 'p_formatted_ip', 'pa', 'k_percent',
                       'bb_percent', 'woba', 'xwoba', 'hard_hit_percent', 'avg_best_speed',
                       'avg_hyper_speed', 'z_swing_miss_percent', 'oz_swing_percent',
                       'whiff_percent', 'swing_percent']
        # case ETLType.REG_SZN:
        #     pass

    clean_sps = clean_sps[columns].drop(columns="SVHD").sort_values("FIP", ascending=True)
    clean_rps = clean_rps[columns].drop(columns="QS").sort_values("FIP", ascending=True)
    return clean_sps, clean_rps
