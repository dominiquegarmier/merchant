from __future__ import annotations

from argparse import ArgumentParser
from argparse import Namespace

from merchant.commands.backtest import backtest_main
from merchant.commands.trade import trade_main
from merchant.commands.train import train_main
from merchant.config import set_config


def parser_args() -> Namespace:
    parser = ArgumentParser()
    return parser.parse_args()


def main() -> int:
    args = parser_args()
    config = set_config(args)  # noqa

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
