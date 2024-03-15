import pandas as pd
import pytest

from mtbl_iokit.read import read


def savant_fixture(pos) -> ():
    """
    Fixture factory
    :return: a function and can accept arg at runtime
    """

    return read.read_in_as(directory="./tests/fixtures",
                           file_name=pos + "_savant",
                           file_type=".csv",
                           as_type=read.IOKitDataTypes.DATAFRAME)


def projections_fixture(pos) -> ():
    """
    Fixture factory
    :return: a function and can accept arg at runtime
    """

    return read.read_in_as(directory="./tests/fixtures",
                           file_name=pos + "_fg",
                           file_type=".csv",
                           as_type=read.IOKitDataTypes.DATAFRAME)


def mock_savant(_, pos) -> pd.DataFrame:
    if pos == "bats":
        return savant_fixture("bats")
    elif pos == "arms":
        return savant_fixture("arms")
    else:
        raise ValueError(f"Unexpected position: {pos}")


def mock_projections(_, pos) -> pd.DataFrame:
    if pos == "bats":
        return projections_fixture("bats")
    elif pos == "arms":
        return projections_fixture("arms")
    else:
        raise ValueError(f"Unexpected position: {pos}")
