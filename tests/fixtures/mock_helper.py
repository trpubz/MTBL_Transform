import pandas as pd
import pytest

from mtbl_iokit.read import read


def savant_fixture(pos, fix_dir="./tests/fixtures") -> ():
    """
    Fixture factory
    :return: a function and can accept arg at runtime
    """
    return read.read_in_as(directory=fix_dir,
                           file_name=pos + "_savant",
                           file_type=".csv",
                           as_type=read.IOKitDataTypes.DATAFRAME)


def fangraphs_fixture(pos, fix_dir="./tests/fixtures") -> ():
    """
    Fixture factory
    :return: a function and can accept arg at runtime
    """
    file_name = pos + "_regular_season" if (
            fix_dir == "./tests/fixtures_reg_szn") else \
        (pos + "_fg")
    return read.read_in_as(directory=fix_dir,
                           file_name=file_name,
                           file_type=".csv",
                           as_type=read.IOKitDataTypes.DATAFRAME)


def mock_savant(_, pos) -> pd.DataFrame:
    if pos == "bats":
        return savant_fixture("bats")
    elif pos == "arms":
        return savant_fixture("arms")
    else:
        raise ValueError(f"Unexpected position: {pos}")


def mock_savant_reg_szn(_, pos) -> pd.DataFrame:
    if pos == "bats":
        return savant_fixture("bats", fix_dir="./tests/fixtures_reg_szn")
    elif pos == "arms":
        return savant_fixture("arms", fix_dir="./tests/fixtures_reg_szn")
    else:
        raise ValueError(f"Unexpected position: {pos}")


def mock_fangraphs(_, pos) -> pd.DataFrame:
    if pos == "bats":
        return fangraphs_fixture("bats")
    elif pos == "arms":
        return fangraphs_fixture("arms")
    else:
        raise ValueError(f"Unexpected position: {pos}")


def mock_fangraphs_reg_szn(_, pos) -> pd.DataFrame:
    if pos == "bats":
        return fangraphs_fixture("bats", fix_dir="./tests/fixtures_reg_szn")
    elif pos == "arms":
        return fangraphs_fixture("arms", fix_dir="./tests/fixtures_reg_szn")
    else:
        raise ValueError(f"Unexpected position: {pos}")