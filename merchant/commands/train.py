from __future__ import annotations

from pprint import pprint
from typing import Any

import exchange_calendars as xcals
import pandas as pd
from ray.rllib.algorithms import ppo  # this causes deprecation warning
from ray.tune.registry import register_env
from tqdm import tqdm

from merchant.data.dataset import SyntheticDataset
from merchant.environment.default.env import TradingEnv


def env_creator(env_config: dict[str, Any]) -> TradingEnv:
    if env_config:
        raise ValueError('env_config not supported')

    xnys = xcals.get_calendar('XNYS')  # NYSE
    start_time = xnys.next_open('2020-2-1') + pd.Timedelta(30, unit='s')

    dataset = SyntheticDataset('2020-1-1', '2020-4-1')
    env = TradingEnv(
        start_time=start_time,
        end_time=pd.Timestamp('2020-3-1'),
        dataset=dataset,
    )
    return env


register_env('trading_env', env_creator)


def train() -> int:
    algo = ppo.PPOConfig().framework('torch').environment('trading_env').build()
    for _ in tqdm(range(5)):
        res = algo.train()
        pprint(res)

    res = algo.evaluate()
    pprint(res)

    return 0
