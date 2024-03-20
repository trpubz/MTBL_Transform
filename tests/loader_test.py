import functools

import pandas as pd
import pytest

from app.src.mtbl_globals import ETLType, DIR_EXTRACT
from app.src.keymap import KeyMap
from app.src.loader import Loader
from tests.fixtures.mock_helper import mock_savant, mock_projections

from mtbl_iokit import read


class TestLoader:
    @pytest.fixture(autouse=True)
    def setup(self):
        # optionally refresh keymap during mods; comment out line if desired to use static file
        # KeyMap.refresh_keymap("./tests/fixtures")
        # setting to alt primary key since testing with preseason data
        keymap = KeyMap("./tests/fixtures", primary_key="FANGRAPHSID").keymap
        yield keymap

    def test_instantiation(self, setup):
        loader = Loader(setup, ETLType.PRE_SZN, "./tests/fixtures")

        assert loader.extract_dir == "./tests/fixtures"
        assert isinstance(loader.keymap, pd.DataFrame)
        assert loader.etl_type == ETLType.PRE_SZN

    def test_load_extracted_data(self, setup, monkeypatch):

        monkeypatch.setattr(Loader, "import_savant", mock_savant)
        monkeypatch.setattr(Loader, "import_projections", mock_projections)

        loader = Loader(setup, ETLType.PRE_SZN, "./tests/fixtures")
        loader.load_extracted_data()

        assert isinstance(loader.combined_bats, pd.DataFrame)
        assert isinstance(loader.combined_arms, pd.DataFrame)

        loader.combined_bats.to_json("./tests/fixtures/combined_bats.json", orient="records")
        loader.combined_arms.to_json("./tests/fixtures/combined_arms.json", orient="records")
