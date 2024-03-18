import os
import argparse

from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS
from app.src.keymap import KeyMap
from app.src.loader import Loader
from app.src.cleaner import clean_hitters, clean_pitchers
from app.src.standardize import z_bats, z_arms
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
    loader.load_extracted_data()
    # clean data
    clean_bats = clean_hitters(loader.combined_bats, etl_type)
    clean_sps, clean_rps = clean_pitchers(loader.combined_arms, etl_type)
    # standardize datasets
    bats = z_bats(clean_bats, LG_RULESET, NO_MANAGERS)
    arms = z_arms(LG_RULESET, NO_MANAGERS, sps=clean_sps, rps=clean_rps)
    # add value assessments
    # export datasets (pd.to_csv), if json, then PlayerKit -- pydantic


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
