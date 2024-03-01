import os
import argparse

from app.src.mtbl_globals import ETLType
from app.src.keymap import KeyMap


def main(etl_type: ETLType):
    keymap = KeyMap().keymap
    # import datasets
    #   league managers
    #   league rules (categories/roster slots)
    #   league players
    #   projections/stats
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
