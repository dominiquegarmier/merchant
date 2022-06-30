from __future__ import annotations

from argparse import Namespace
from typing import NamedTuple


class Config(NamedTuple):
    pass


def get_config(ns: Namespace) -> Config:
    return Config()
