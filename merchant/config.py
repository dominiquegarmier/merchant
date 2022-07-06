from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    DATA_PATH: Path = Path(__file__).parent.parent / 'data'


GLOBAL_CONFIG = Config()


def set_config(ns: Namespace) -> Config:
    # mutation GLOBAL_CONFIG
    return GLOBAL_CONFIG


def get_config() -> Config:
    return GLOBAL_CONFIG
