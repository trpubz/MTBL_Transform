from unittest import case

import pandas as pd

from app.src.mtbl_globals import ETLType


class Cleaner:
    def __init__(self, etl_type: ETLType, bats: pd.DataFrame, arms: pd.DataFrame) -> None:
        """
        :param etl_type: ETLType to clean
        :param bats: combined df for bats
        :param arms: combined df for arms
        """
        self.etl_type = etl_type
        self.bats = bats
        self.arms = arms

    def clean_hitters(self) -> pd.DataFrame:
        """
        Clean hitters, remove unnecessary columns, sort, and get data ready for standardization
        :return: cleaned bats dataframe
        """
        self.bats["proj_SBN"] = self.bats["proj_SB"] - self.bats["proj_CS"]
        self.bats["SBN"] = self.bats["r_total_stolen_base"] - self.bats["r_total_caught_stealing"]
        clean_bats = None
        columns = ['ESPNID', 'FANGRAPHSID', 'MLBID', 'name', 'team', 'positions',
                   'proj_G', 'proj_PA', 'proj_AB', 'proj_H', 'proj_HR', 'proj_R', 'proj_RBI',
                   'proj_SBN', 'proj_AVG', 'proj_OBP', 'proj_SLG', 'proj_OPS', 'proj_BB%', 'proj_K%',
                   'proj_wOBA', 'proj_ISO', 'proj_BABIP', 'proj_wRC', 'proj_wRAA', 'proj_wRC+',
                   'proj_WAR',
                   'pa', 'xslg', 'woba', 'xwoba', 'xobp', "sweet_spot_percent",
                   "barrel_batted_rate", "hard_hit_percent", "avg_best_speed", "avg_hyper_speed",
                   "oz_swing_percent", "n_bolts", "xwOBA_diff", "xSLG_diff", "xOBP_diff"
                   ]
        sort_value = ""

        match self.etl_type:
            case ETLType.PRE_SZN:
                columns += ['year']
                sort_value = "proj_wRC+"
            case ETLType.REG_SZN:
                columns += ['owner',
                            'prtr_%ROST', 'prtr_PRTR', 'prtr_HR', 'prtr_R',
                            'prtr_RBI', 'prtr_SBN', 'prtr_OBP', 'prtr_SLG',
                            'G', 'PA', 'HR', 'R', 'RBI', 'SBN', 'AVG', 'OBP', 'SLG',
                            'on_base_plus_slg', 'BB%', 'K%', 'wOBA', 'ISO', 'BABIP',
                            'wRC+', 'WAR'
                            ]
                sort_value = "wRC+"

        clean_bats = self.bats[columns].sort_values(by=sort_value, ascending=False)

        return clean_bats

    def clean_pitchers(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Clean pitchers, remove unnecessary columns, sort, and get data ready for standardization
        :return: tuple of dataframes containing SPs and RPs
        """
        self.arms["proj_SVHD"] = self.arms["proj_SV"] + self.arms["proj_HLD"]
        clean_sps = self.arms[self.arms["proj_QS"] > self.arms["proj_SVHD"]]
        clean_rps = self.arms[self.arms["proj_SVHD"] >= self.arms["proj_QS"]]
        columns = ['ESPNID', 'FANGRAPHSID', 'MLBID', 'name', 'team', 'positions',
                   'proj_G', 'proj_GS', 'proj_IP', 'proj_QS', 'proj_SVHD',
                   'proj_ERA', 'proj_WHIP', 'proj_K/9', 'proj_FIP', 'proj_BB/9', 'proj_K/BB',
                   'proj_HR/9', 'proj_BABIP', 'proj_WAR',
                   'woba', 'xwoba', "xwOBA_diff", 'hard_hit_percent', 'avg_best_speed',
                   'avg_hyper_speed', 'whiff_percent', 'swing_percent']
        sort_value = ""

        match self.etl_type:
            case ETLType.PRE_SZN:
                columns += ["year"]
                sort_value = "proj_FIP"
            case ETLType.REG_SZN:
                columns += ['owner',
                            'prtr_%ROST', "prtr_PRTR", "prtr_IP", "prtr_QS", "prtr_ERA",
                            "prtr_WHIP", "prtr_K/9", "prtr_SVHD",
                            'G', 'GS', 'IP', 'ERA', 'WHIP', 'K/9', 'p_save', 'p_hold', 'SVHD',
                            'p_quality_start', 'FIP', 'BB/9', 'HR/9', 'BABIP', 'WAR',
                            'k_percent', 'bb_percent']
                sort_value = "FIP"

        clean_sps = clean_sps[columns].drop(columns="proj_SVHD").sort_values(sort_value,
                                                                             ascending=True)
        clean_rps = clean_rps[columns].drop(columns="proj_QS").sort_values(sort_value,
                                                                           ascending=True)

        return clean_sps, clean_rps
