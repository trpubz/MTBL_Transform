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
            # set columns
            pass
        # case ETLType.REG_SZN:
        #     pass

    df = df[columns].sort_values(by="wRAA", ascending=False)

    return clean_bats


def clean_pitchers(df: pd.DataFrame, etl_type: ETLType) -> (pd.DataFrame, pd.DataFrame):
    """
    Clean pitchers, remove unnecessary columns, sort, and get data ready for standardization
    :param df: dataframe containing combined arms
    :param etl_type: PRESZN or REGSZN
    :return: tuple of dataframes containing SPs and RPs
    """
    clean_sps = None
    clean_rps = None

    return clean_sps, clean_rps
