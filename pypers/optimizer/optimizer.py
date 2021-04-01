from __future__ import annotations

from typing import Optional

from pypers import field, Workspace
from pypers.fwi.workspace import create, FWIKwargs, ASDFKwargs


class Optimizer(Workspace):
    # number of iterations
    niters: int = field()

    # reduce smoothing radius over iterations
    smooth_decay: Optional[float] = field()


def create_optimizer(cwd: str = '.', kwargs: FWIKwargs = {}) -> Optimizer:
    return create('optimizer', cwd, kwargs)
