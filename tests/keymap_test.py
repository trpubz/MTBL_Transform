import csv
import json
import os
import tempfile
import time

import pandas as pd
import pytest
import pytest_benchmark

from app.src.keymap import KeyMap


class TestKeymap:
    @pytest.fixture
    def km(self):
        return KeyMap("tests/fixtures").keymap

    def test_keymap(self, km):
        assert isinstance(km, pd.DataFrame)
        assert km.loc["42404"]["PLAYERNAME"] == "Corbin Carroll"

    def test_indexing_json(self, benchmark):
        def indexing_json():
            json_keymap = json.load(open("tests/fixtures/mtbl_keymap.json"))["data"]
            key = "42404"  # Corbin Carroll

            json_result = None
            for player in json_keymap:
                if player["ESPNID"] == key:
                    json_result = player["PLAYERNAME"]

            return json_result

        result = benchmark(indexing_json)
        assert result is not None

    def test_indexing_pandas(self, km, benchmark):
        def indexing_pandas():
            pd_result = km.loc["42404"]["PLAYERNAME"]

            return pd_result

        result = benchmark(indexing_pandas)
        assert result is not None

    # @pytest.mark.skip(reason="network call")
    def test_refresh_keymap(self):
        test_dir = "./tests/fixtures"
        KeyMap.refresh_keymap(test_dir)

        expected_file = "mtbl_keymap.json"  # Example filename
        file_path = os.path.join(test_dir, expected_file)

        # Assertion to check for file existence
        assert os.path.exists(file_path)
