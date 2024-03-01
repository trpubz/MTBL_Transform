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
    def __init__(self):
        self.keymap = None
        self.load_keymap()
        verify_transform_dir()

    def load_keymap(self):
        """
        Load keymap from directory
        :return: None
        """
        # make DIR_EXTRACT if it doesn't exist
        with open(os.path.join(DIR_EXTRACT, "mtbl_keymap.json")) as f:
            self.keymap = pd.read_json(f).set_index("ESPNID")

    @staticmethod
    def refresh_keymap():
        """
        Static method to refresh keymap, compares timestamps to determine if fetching is necessary
        :return:
        """
        # TODO:
        pass


def verify_transform_dir():
    """
    Checks if a directory exists, and creates it if it doesn't.
    """
    if not os.path.exists(DIR_TRANSFORM):
        os.makedirs(DIR_TRANSFORM)