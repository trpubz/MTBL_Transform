import pytest
import pandas as pd

from mtbl_iokit.read import read

from app.src.mtbl_globals import ETLType
from app.src import cleaner
from app.src.loader import Loader
from app.src.keymap import KeyMap
from tests.fixtures.mock_helper import mock_savant, mock_projections


class TestCleaner:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch):
        keymap = KeyMap("./tests/fixtures", primary_key="FANGRAPHSID").keymap

        monkeypatch.setattr(Loader, "import_savant", mock_savant)
        monkeypatch.setattr(Loader, "import_projections", mock_projections)

        loader = Loader(keymap, ETLType.PRE_SZN)
        loader.load_extracted_data()

    def test_clean_hitters(self):
        assert True
