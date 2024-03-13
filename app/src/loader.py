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
    def __init__(self,
                 keymap: pd.DataFrame,
                 etl_type: ETLType,
                 extract_dir: str = DIR_EXTRACT):
        """
        Loader constructor based on where to load data from and the 'shape' it should take (pre
        or reg season)
        :param keymap: pd.DataFrame containing the keymap
        :param etl_type: enum holding the types of extracted data; PRE_SZN or REG_SZN
        :param extract_dir: string path where extracted data will be fetched from
        """
        self.combined_bats = None
        self.combined_arms = None
        self.extract_dir = extract_dir
        dfs_bats = {}
        dfs_arms = {}
        match etl_type:
            case ETLType.PRE_SZN:
                dfs_bats["FANGRAPHS"] = self.import_projections("bats")
                dfs_bats["SAVANT"] = self.import_savant("bats")
                dfs_arms["FANGRAPHS"] = self.import_projections("arms")
                dfs_arms["SAVANT"] = self.import_savant("arms")
            case ETLType.REG_SZN:
                # import managers, player universe, stats, ros projections, savant
                # TODO:
                pass

        self.combine_dataframes(keymap, dfs_bats, dfs_arms)

    def import_owners(self):
        # TODO:
        pass

    def import_ruleset(self):
        # TODO:
        pass

    def import_savant(self, pos) -> pd.DataFrame:
        return read.read_in_as(directory=self.extract_dir,
                               file_name=pos + "_savant",
                               file_type=".csv",
                               as_type=read.IOKitDataTypes.DATAFRAME)

    def import_projections(self, pos) -> pd.DataFrame:
        return read.read_in_as(directory=self.extract_dir,
                               file_name=pos + "_fg",
                               file_type=".csv",
                               as_type=read.IOKitDataTypes.DATAFRAME)

    def import_stats(self, pos):
        pass

    def combine_dataframes(self, keymap, dfs_bats: dict, dfs_arms):
        """
        Combines the pos group lists.
        :param keymap: pd.Dataframe containing the joins table
        :param dfs_bats: dict of Dataframes for hitters
        :param dfs_arms: dict of Dataframes for pitchers
        :return: None
        """
        combined_bats = None
        for source, df_bats in dfs_bats.items():
            # Assuming 'source_key' is the column in dfs_bats with the source-specific primary key
            # Assuming 'combined_key' is the column in keymap with the aligned key
            match source:
                case "FANGRAPHS":
                    source_key = "PlayerId"
                    keymap_key = "FANGRAPHSID"
                    aux_key = "MLBID"  # this will match during concat sequence
                case "SAVANT":
                    source_key = "player_id"
                    keymap_key = "MLBID"
                    aux_key = "FANGRAPHSID"

            try:
                merged_df = df_bats.merge(keymap[[keymap_key, aux_key]],
                                          how="left",
                                          left_on=source_key,
                                          right_on=keymap_key)

                # Combine with the existing DataFrame (handles first iteration and subsequent ones)
                # TODO: looking at a merge here
                combined_bats = pd.concat(
                    [combined_bats, merged_df]) if combined_bats is not None else merged_df
            except AttributeError as e:
                print(e)

        self.combined_bats = combined_bats


