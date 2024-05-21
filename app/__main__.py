import argparse

from mtbl_iokit.write import export_dataframe
from app.src.mtbl_globals import ETLType, LG_RULESET, NO_MANAGERS, DIR_TRANSFORM
from app.src.keymap import KeyMap
from app.src.loader import Loader
from app.src.cleaner import Cleaner
from app.src.transformer import Transformer
from app.src.appraiser import Appraiser


def main(etl_type: ETLType):
    """
    Main controller.
    Note: if ETLType is PRE_SZN, keymap primary key should be set to other than ESPNID.
    :param etl_type: Enum for PRE_SZN or REG_SZN
    """
    km = KeyMap(primary_key="FANGRAPHSID").keymap  # object has keymap attribute
    loader = Loader(keymap=km, etl_type=etl_type)  # object has combined dfs
    loader.load_extracted_data()
    # clean data
    cleaner = Cleaner(etl_type=etl_type, bats=loader.combined_bats, arms=loader.combined_arms)
    clean_bats = cleaner.clean_hitters()
    clean_sps, clean_rps = cleaner.clean_pitchers()
    # standardize datasets
    transformer = Transformer(ruleset=LG_RULESET,
                              no_managers=NO_MANAGERS,
                              bats=clean_bats,
                              sps=clean_sps,
                              rps=clean_rps)

    bats = transformer.z_bats()
    arms = transformer.z_arms()
    # bats and arms are now keyed by pos
    all_players = bats.copy().update(arms)

    budget_pref = {
        "bats": {
            "ovr": 0.65,
            "cats": {
                "HR": 0.20,
                "R": 0.15,
                "RBI": 0.10,
                "SBN": 0.15,
                "OBP": 0.20,
                "SLG": 0.20
            }
        },
        "sps": {
            "ovr": 0.20,
            "cats": {
                "IP": 0.15,
                "QS": 0.20,
                "ERA": 0.20,
                "WHIP": 0.20,
                "K/9": 0.25
            }
        },
        "rps": {
            "ovr": 0.15,
            "cats": {
                "IP": 0.15,
                "SVHD": 0.20,
                "ERA": 0.20,
                "WHIP": 0.20,
                "K/9": 0.25
            }
        }
    }
    app = Appraiser(ruleset=LG_RULESET,
                    no_managers=NO_MANAGERS,
                    budget_split=budget_pref,
                    bats=bats, arms=arms)
    app.calculate_league_batting_category_totals()
    app.calculate_batting_category_weights_shekels()
    app.calculate_pitching_category_weights_shekels()
    app.add_skekels()

    for pos, pos_group in app.pos_groups.items():
        export_dataframe(pos_group["players"], "mtbl_" + pos.lower(), ".json", DIR_TRANSFORM)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MTBL Transform args")
    parser.add_argument(
        "--etl-type",
        type=ETLType.from_string,
        choices=list(ETLType),
        help="ETL Type; PRE_SZN or REG_SZN",
        default=ETLType.REG_SZN)

    args = parser.parse_args()
    main(args.etl_type)
