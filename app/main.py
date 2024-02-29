import os
import argparse

from app.src.mtbl_globals import ETLType


def main(etl_type: ETLType):
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
