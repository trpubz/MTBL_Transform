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
        clean_bats = None
        columns = ['ESPNID', 'FANGRAPHSID', 'MLBID', 'name', 'team', 'positions',
                   ]
        match self.etl_type:
            case ETLType.PRE_SZN:
                columns.append(['G', 'PA', 'AB', 'H', 'HR', 'R', 'RBI', 'SB', 'CS', 'SBN', 'AVG', 'OBP', 'SLG', 'OPS',
                                'BB%', 'K%', 'wOBA', 'ISO', 'BABIP', 'wRC', 'wRAA', 'wRC+',
                                'WAR',
                                'attempts', 'avg_hit_angle', 'anglesweetspotpercent',
                                'max_hit_speed', 'avg_hit_speed', 'ev50', 'fbld', 'gb',
                                'max_distance', 'avg_distance', 'avg_hr_distance', 'ev95plus',
                                'ev95percent', 'barrels', 'brl_percent', 'brl_pa'])
            case ETLType.REG_SZN:
                columns.append(['owner',
                                'prtr_%ROST', 'prtr_PRTR', 'prtr_H', 'prtr_HR', 'prtr_R',
                                'prtr_RBI', 'prtr_SBN', 'prtr_OBP', 'prtr_SLG'])

        self.bats["SBN"] = self.bats["SB"] - self.bats["CS"]
        clean_bats = self.bats[columns].sort_values(by="wRAA", ascending=False)

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
                   'avg_hyper_speed', 'z_swing_miss_percent', 'oz_swing_percent', 'whiff_percent',
                   'swing_percent']
        sort_value = ""

        match self.etl_type:
            case ETLType.PRE_SZN:
                columns.append(["year"])
                sort_value = "proj_FIP"
            case ETLType.REG_SZN:
                columns.append(['owner',
                                'prtr_%ROST', "prtr_PRTR", "prtr_IP", "prtr_QS", "prtr_ERA",
                                "prtr_WHIP", "prtr_K/9", "prtr_SVHD",
                                'G', 'GS', 'IP', 'ERA', 'WHIP', 'K/9', 'p_save', 'p_hold', 'SVHD',
                                'p_quality_start', 'FIP', 'BB/9', 'HR/9', 'BABIP', 'WAR',
                                'k_percent', 'bb_percent'])
                sort_value = "FIP"

        clean_sps = clean_sps[columns].drop(columns="proj_SVHD").sort_values(sort_value,
                                                                             ascending=True)
        clean_rps = clean_rps[columns].drop(columns="proj_QS").sort_values(sort_value,
                                                                           ascending=True)

        return clean_sps, clean_rps
