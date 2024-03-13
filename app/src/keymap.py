"""
Object encapsulation for keymap in-memory access
by: pubins.taylor
date: 29 FEB 2024
keymap.py
"""
import os

import pandas as pd

from app.src.mtbl_globals import DIR_EXTRACT, DIR_TRANSFORM


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
            # Setting drop=False prevents the removal of the primary key column,
            # retaining it along with its name within the DataFrame.
            self.keymap = pd.read_json(f).set_index(primary_key, drop=False)
            self.keymap.index.name = "idx" + primary_key
            self.keymap["MLBID"] = self.keymap["MLBID"].astype(str)

    @staticmethod
    def refresh_keymap():
        """
        Static method to refresh keymap, compares timestamps to determine if fetching is necessary
        :return:
        """
        # TODO:
        pass


def verify_transform_dir(output_dir=DIR_TRANSFORM):
    """
    Checks if a directory exists, and creates it if it doesn't.
    Required to have write permissions to /User directory
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
