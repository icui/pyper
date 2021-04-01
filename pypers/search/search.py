from __future__ import annotations

from typing import Optional

from pypers import field, Workspace
from pypers.fwi.workspace import create, FWIKwargs, ASDFKwargs


class Search(Workspace):
    # maximum number of search steps
    nsteps: int = field()

    # initial step length
    step_init: float = field()

    # path to initial misfit value
    path_misfit: str = field()


def create_search(cwd: str = '.', kwargs: FWIKwargs = {}) -> Search:
    return create('search', cwd, kwargs)
