"""
Loader module, loads extracted data.
Loader has attributes for combined_bats and combined_arms
by: pubins.taylor
date: 29 FEB 2024
"""
import os

import pandas as pd

from mtbl_iokit import read

from app.src.mtbl_globals import ETLType, DIR_EXTRACT


class Loader:
    def __init__(self, etl_type: ETLType, extract_dir: str = DIR_EXTRACT):
        self.combined_bats = None
        self.combined_arms = None
        self.extract_dir = extract_dir
        dfs_bats = []
        dfs_arms = []
        match etl_type:
            case ETLType.PRE_SZN:
                for pos in ["bats", "arms"]:
                    dfs_bats.append(self.import_projections(pos))
                    dfs_arms.append(self.import_savant(pos))
            case ETLType.REG_SZN:
                # import managers, player universe, stats, ros projections, savant
                pass

        self.combine_dataframes()

    def import_owners(self):
        # TODO:
        pass

    def import_ruleset(self):
        # TODO:
        pass

    def import_savant(self, pos):
        return read.read_in_as(directory=self.extract_dir,
                               file_name=pos + "_savant",
                               file_type=".csv",
                               as_type=read.IOKitDataTypes.DATAFRAME)

    def import_projections(self, pos):
        return read.read_in_as(directory=self.extract_dir,
                               file_name=pos+"_fg",
                               file_type=".json",
                               as_type=read.IOKitDataTypes.DATAFRAME)

    def import_stats(self, pos):
        pass

    def combine_dataframes(self):
        pass
