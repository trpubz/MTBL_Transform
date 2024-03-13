import functools

import pandas as pd
import pytest

from app.src.mtbl_globals import ETLType, DIR_EXTRACT
from app.src.keymap import KeyMap
from app.src.loader import Loader

from mtbl_iokit import read


class TestLoader:
    @pytest.fixture(autouse=True)
    def setup(self):
        # setting to alt primary key since testing with preseason data
        keymap = KeyMap("./tests/fixtures", primary_key="FANGRAPHSID").keymap
        yield keymap

    @pytest.fixture
    def savant_fixture(self) -> ():
        """
        Fixture factory
        :return: a function and can accept arg at runtime
        """
        def _savant_fixture(pos) -> pd.DataFrame:
            return read.read_in_as(directory="./tests/fixtures",
                            file_name=pos + "_savant",
                            file_type=".csv",
                            as_type=read.IOKitDataTypes.DATAFRAME)

        return _savant_fixture

    @pytest.fixture
    def projections_fixture(self) -> ():
        """
        Fixture factory
        :return: a function and can accept arg at runtime
        """
        def _projections_fixture(pos) -> pd.DataFrame:
            return read.read_in_as(directory="./tests/fixtures",
                            file_name=pos + "_fg",
                            file_type=".csv",
                            as_type=read.IOKitDataTypes.DATAFRAME)

        return _projections_fixture

    def test_instantiation(self, setup, monkeypatch, savant_fixture, projections_fixture):
        def mock_savant(_, pos) -> pd.DataFrame:
            if pos == "bats":
                return savant_fixture("bats")
            elif pos == "arms":
                return savant_fixture("arms")
            else:
                raise ValueError(f"Unexpected position: {pos}")

        def mock_projections(_, pos) -> pd.DataFrame:
            if pos == "bats":
                func = projections_fixture
                return func("bats")
            elif pos == "arms":
                func = projections_fixture
                return func("arms")
            else:
                raise ValueError(f"Unexpected position: {pos}")

        monkeypatch.setattr(Loader, "import_savant", mock_savant)
        monkeypatch.setattr(Loader, "import_projections", mock_projections)

        loader = Loader(setup, ETLType.PRE_SZN)

        assert loader.extract_dir == DIR_EXTRACT
        assert isinstance(loader.combined_bats, pd.DataFrame)
        # assert isinstance(loader.combined_arms, pd.DataFrame)
