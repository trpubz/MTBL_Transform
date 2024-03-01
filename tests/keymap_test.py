import csv
import json
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
        assert km.loc[42404]["PLAYERNAME"] == "Corbin Carroll"

    def test_keymap_benchmark(self, km):
        json_keymap = json.load(open("tests/fixtures/mtbl_keymap.json"))

        csv_keymap = csv.DictReader(open("tests/fixtures/mtbl_keymap.csv"))
        # Simple Key-Based Search
        key = "42404"  # Corbin Carroll

        start_time = time.time()
        # Perform search in JSON
        json_result = None
        for player in json_keymap:
            if player["ESPNID"] == key:
                json_result = player["PLAYERNAME"]

        assert json_result is not None
        end_time = time.time()
        json_time = end_time - start_time

        start_time = time.time()
        csv_result = None
        for row in csv_keymap:
            if row["ESPNID"] == key:
                csv_result = row["PLAYERNAME"]

        assert csv_result is not None
        end_time = time.time()
        csv_time = end_time - start_time

        start_time = time.time()
        pd_result = km.loc[42404]["PLAYERNAME"]

        assert pd_result is not None
        end_time = time.time()
        pd_time = end_time - start_time

        print()
        print(f"JSON Search Time: {json_time * 1000}ms")
        print(f"CSV Search Time: {csv_time * 1000}ms")
        print(f"Pandas Search Time: {pd_time * 1000}ms")
        assert True

    def test_indexing_json(self, benchmark):
        def indexing_json():
            json_keymap = json.load(open("tests/fixtures/mtbl_keymap.json"))
            key = "42404"  # Corbin Carroll

            json_result = None
            for player in json_keymap:
                if player["ESPNID"] == key:
                    json_result = player["PLAYERNAME"]

            return json_result

        result = benchmark(indexing_json)
        assert result is not None

    def test_indexing_csv(self, benchmark):
        def indexing_csv():
            csv_keymap = csv.DictReader(open("tests/fixtures/mtbl_keymap.csv"))
            key = "42404"  # Corbin Carroll

            csv_result = None
            for row in csv_keymap:
                if row["ESPNID"] == key:
                    csv_result = row["PLAYERNAME"]

            return csv_result

        result = benchmark(indexing_csv)
        assert result is not None

    def test_indexing_pandas(self, km, benchmark):
        def indexing_pandas():
            pd_result = km.loc[42404]["PLAYERNAME"]

            return pd_result

        result = benchmark(indexing_pandas)
        assert result is not None
