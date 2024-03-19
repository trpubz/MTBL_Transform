"""
Object encapsulation for keymap in-memory access
by: pubins.taylor
date: 29 FEB 2024
keymap.py
"""
import io
import json
import os

import numpy as np
import pandas as pd

from app.src.mtbl_globals import DIR_EXTRACT, DIR_TRANSFORM, MTBL_KEYMAP_URL
from mtbl_iokit.write import write


class KeyMap:
    def __init__(self, keymap_dir=DIR_EXTRACT, primary_key: str = "ESPNID"):
        self.keymap = None
        self.load_keymap(keymap_dir, primary_key)

        if keymap_dir == DIR_TRANSFORM:
            verify_transform_dir()

    def load_keymap(self, keymap_dir: str, primary_key: str) -> None:
        """
        Load keymap from directory
        :param keymap_dir: containing keymap file
        :param primary_key: sets the primary key of the keymap.
            Options: ESPNID, MLBID, FANGRAPHSID, BREFID
        :return: None
        """
        # make DIR_EXTRACT if it doesn't exist
        with open(os.path.join(keymap_dir, "mtbl_keymap.json")) as f:
            # keymap is loaded with schema, need to access data key
            json_data = json.load(f)["data"]
            # Setting drop=False prevents the removal of the primary key column,
            # retaining it along with its name within the DataFrame.
            keymap = pd.read_json(io.StringIO(json.dumps(json_data)))
            keymap = convert_num_id_cols(keymap)

            keymap.set_index(primary_key, drop=False, inplace=True)
            keymap.index.name = "idx" + primary_key

            self.keymap = keymap

    @staticmethod
    def refresh_keymap(save_dir: str = DIR_EXTRACT):
        """
        Static method to refresh keymap, compares timestamps to determine if fetching is necessary
        :param save_dir: directory to save file
        :return:
        """
        # read html appends each table to list, access dataframe with index
        new_keymap = pd.read_html(MTBL_KEYMAP_URL, header=1)[0]
        # reset index, drop index column, remove bad rows
        new_keymap = new_keymap.reset_index(drop=True).drop(columns="1").dropna(how='all')

        new_keymap = convert_num_id_cols(new_keymap)

        write.export_dataframe(new_keymap, "mtbl_keymap", ".json", save_dir)


def convert_num_id_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the columns that load in as floats or integers into strings
    :param df: the dataframe to convert
    :return: a dataframe with only the columns known to load as numerical values
    """
    def float_to_str(x):
        if pd.notna(x).any():  # Check for at least one non-null value

            for idx in range(len(x)):
                if isinstance(x.iloc[idx], float) and not pd.isnull(x.iloc[idx]):
                    x.iloc[idx] = str(int(x.iloc[idx]))

            return x
        else:
            return x

    df[["MLBID", "ESPNID"]] = df[["MLBID", "ESPNID"]].apply(float_to_str, axis=1)

    return df


def verify_transform_dir(output_dir=DIR_TRANSFORM):
    """
    Checks if a directory exists, and creates it if it doesn't.
    Required to have write permissions to /User directory
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
