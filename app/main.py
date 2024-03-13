import os
import argparse

from app.src.mtbl_globals import ETLType
from app.src.keymap import KeyMap
from app.src.loader import Loader
import app.src.transform as trx


def main(etl_type: ETLType):
    """
    Main controller.
    Note: if ETLType is PRE_SZN, keymap primary key should be set to other than ESPNID.
    :param etl_type:
    :return:
    """
    km = KeyMap(primary_key="FANGRAPHSID")  # object has keymap attribute
    loader = Loader(km.keymap, etl_type=ETLType.PRE_SZN)  # object has combined dfs

    # combine datasets (could be done in pandas)
    # normalize/standardize datasets
    # add data fields
    # export datasets (pd.to_csv), if json, then PlayerKit -- pydantic

    print(etl_type)
    pass


if __name__ == '__main__':
    print(list(ETLType.__members__))
    parser = argparse.ArgumentParser(description="MTBL Transform args")
    parser.add_argument(
        "--etl-type",
        type=ETLType.from_string,
        choices=list(ETLType),
        help="ETL Type; PRE_SZN or REG_SZN",
        default=ETLType.REG_SZN)

    args = parser.parse_args()
    main(args.etl_type)
