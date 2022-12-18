from __future__ import annotations

from abc import ABCMeta

import pandas as pd


class Action(metaclass=ABCMeta):
    ...


class ActionException(Exception, metaclass=ABCMeta):
    _action: Action


class ActionExecution(metaclass=ABCMeta):
    _action: Action
    _exceptions: list[ActionException]

    _success: bool
    _completed_at: pd.Timestamp
