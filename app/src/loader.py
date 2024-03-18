"""
Loader module, loads extracted data.
Loader has attributes for combined_bats and combined_arms
by: pubins.taylor
date: 13 MAR 2024
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
        self.player_universe = None
        self.extract_dir = extract_dir
        self.keymap = keymap
        self.etl_type = etl_type

    def load_extracted_data(self) -> None:
        """
        Loads and combines extracted data
        :return: None
        """
        dfs_bats = {}
        dfs_arms = {}
        self.import_universe()
        match self.etl_type:
            case ETLType.PRE_SZN:
                # order of keys matters here since SAVANT processing relies on FANGRAPHSID
                dfs_bats["FANGRAPHS"] = self.import_projections("bats")
                dfs_bats["SAVANT"] = self.import_savant("bats")
                dfs_arms["FANGRAPHS"] = self.import_projections("arms")
                dfs_arms["SAVANT"] = self.import_savant("arms")

            case ETLType.REG_SZN:
                # import managers, player universe, stats, ros projections, savant
                # TODO:
                pass

        self.combine_dataframes(dfs_bats, dfs_arms)

    # def import_owners(self):
    # TODO:
    # pass

    # def import_ruleset(self):
    # TODO:
    # pass

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

    def import_universe(self):
        self.player_universe = read.read_in_as(directory=self.extract_dir,
                                               file_name="espn_player_universe",
                                               file_type=".json",
                                               as_type=read.IOKitDataTypes.DATAFRAME)

    def combine_dataframes(self, dfs_bats: dict, dfs_arms: dict) -> None:
        """
        Combines the pos group lists.  Also adds the Player Universe Positions
        :param keymap: pd.Dataframe containing the joins table
        :param dfs_bats: dict of Dataframes for hitters
        :param dfs_arms: dict of Dataframes for pitchers
        :return: None
        """

        def combine_pos_group(pos: dict) -> pd.DataFrame:
            combined = None
            if self.etl_type == ETLType.PRE_SZN:
                frame = self.player_universe
                combined = frame[["name", "team", "positions", "espn_id"]]

            for source, df in pos.items():
                # Assuming 'source_key' is the column in dfs_bats with the source-specific
                # primary key Assuming 'combined_key' is the column in keymap with the aligned key
                match source:
                    case "FANGRAPHS":
                        source_key = "PlayerId"
                        keymap_key = "FANGRAPHSID"
                        aux_key = "MLBID"  # this will match during merge sequence
                    case "SAVANT":
                        source_key = "player_id"
                        keymap_key = "MLBID"
                        aux_key = "FANGRAPHSID"

                try:
                    # add the keys from the keymap, to include ESPN Player Universe Keys
                    keyed_df = df.merge(self.keymap[[keymap_key, aux_key, "ESPNID"]],
                                        how="left",
                                        left_on=source_key,
                                        right_on=keymap_key)

                    if source == "SAVANT":
                        # will raise AttributeError if there is a key problem
                        check_keymap_validity(keyed_df, aux_key, source)
                    else:
                        check_keymap_validity(keyed_df, keymap_key, source)

                    # Combine with the existing DataFrame (handles first iteration and subsequent
                    # ones)
                    combined = combined.merge(
                        keyed_df, how="left", left_on="espn_id", right_on="ESPNID") if (
                            combined is not None) else keyed_df
                except AttributeError as e:
                    print(e)

            combined["MLBID"] = combined["MLBID_x"]
            combined.drop(columns=["MLBID_x", "MLBID_y"], inplace=True)
            combined["FANGRAPHSID"] = combined["FANGRAPHSID_x"]
            combined.drop(columns=["FANGRAPHSID_x", "FANGRAPHSID_y"], inplace=True)
            combined["ESPNID"] = combined["ESPNID_x"]
            combined.drop(columns=["ESPNID_x", "ESPNID_y"], inplace=True)
            return combined

        # dropna for ESPNID since we don't have those keys and not in the relevant universe
        # TODO: drop _x/_y columns or combine them earlier
        self.combined_bats = combine_pos_group(dfs_bats).dropna(subset="ESPNID")
        self.combined_arms = combine_pos_group(dfs_arms).dropna(subset="ESPNID")


def check_keymap_validity(df: pd.DataFrame, id_col: str, source: str) -> None:
    """
    Checks if the keymap is valid.
    Savant data is only available from pro players which means a savant df cannot have a 'sa' prefix
     in the FANGRAPHSID column.
    Fangraphs data for projections cannot have an empty value after merging with keymap; this means
     player is not in my keymap.
    :param df: dataframe to check, typically savant keyed df.
    :param id_col: the name of the column to check
    :param source: SAVANT, FANGRAPHS
    :raise: AttributeError if players have bad keys
    :return:
    """
    problematic_players = None

    match source:
        case "FANGRAPHS":
            problematic_players = df[df[id_col].isna()]["Name"]
        case "SAVANT":
            # FANGRAPHSID cannot start with 'sa' and be found in the savant data.
            no_nas_df = df[df[id_col].notna()]
            problematic_players = no_nas_df[no_nas_df["FANGRAPHSID"].str.startswith(
                'sa')]["last_name, first_name"]

    if not problematic_players.empty:
        error_msg = ("Keymap Error: The following players have problematic IDs (starting with "
                     "'sa'):\n{}").format(
            '\n'.join(problematic_players.tolist())
        )
        raise AttributeError(error_msg)
