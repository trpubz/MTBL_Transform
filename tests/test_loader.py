import pandas as pd
import pytest

from app.src.mtbl_globals import ETLType
from app.src.keymap import KeyMap
from app.src.loader import Loader


class TestLoader:
    @pytest.fixture
    def setup_pre_szn(self):
        # optionally refresh keymap during mods; comment out line if desired to use static file
        # KeyMap.refresh_keymap("./tests/fixtures")
        # setting to alt primary key since testing with preseason data
        keymap = KeyMap("./tests/fixtures", primary_key="FANGRAPHSID").keymap
        yield keymap

    @pytest.fixture
    def setup_reg_szn(self):
        keymap = KeyMap("./tests/fixtures_reg_szn", primary_key="FANGRAPHSID").keymap
        yield keymap

    def test_instantiation(self, setup_pre_szn):
        loader = Loader(setup_pre_szn, ETLType.PRE_SZN, "./tests/fixtures")

        assert loader.extract_dir == "./tests/fixtures"
        assert isinstance(loader.keymap, pd.DataFrame)
        assert loader.etl_type == ETLType.PRE_SZN

    def test_load_extracted_data_pre_szn(self, setup_pre_szn):

        loader = Loader(setup_pre_szn, ETLType.PRE_SZN, "./tests/fixtures")
        loader.load_extracted_data()

        assert isinstance(loader.combined_bats, pd.DataFrame)
        assert isinstance(loader.combined_arms, pd.DataFrame)

        loader.combined_bats.to_json("./tests/fixtures/combined_bats.json",
                                     orient="records", indent=2)
        loader.combined_arms.to_json("./tests/fixtures/combined_arms.json",
                                     orient="records", indent=2)

    def test_load_extracted_data_reg_szn(self, setup_reg_szn):
        loader = Loader(setup_reg_szn, ETLType.REG_SZN, "./tests/fixtures_reg_szn")
        loader.load_extracted_data()

        assert isinstance(loader.combined_bats, pd.DataFrame)
        assert isinstance(loader.combined_arms, pd.DataFrame)

        loader.combined_bats.to_json("./tests/fixtures_reg_szn/combined_bats.json",
                                     orient="records", indent=2)
        loader.combined_arms.to_json("./tests/fixtures_reg_szn/combined_arms.json",
                                     orient="records", indent=2)
