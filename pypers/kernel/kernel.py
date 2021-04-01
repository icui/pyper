from typing import List, Optional

from pypers import Workspace, field
from pypers.fwi.workspace import create, FWIKwargs


class Kernel(Workspace):
    # current iteration
    iteration: int = field(0)

    # alter RNG seed
    seed: int = field(0)

    # path to initial model
    path_model: Optional[str] = field()

    # period range for kernel computation
    period_range: List[float] = field(required=True)

    # compute only misfit (skip kernel computation)
    misfit_only: bool = field(False)


def create_kernel(cwd: str = '.', kwargs: FWIKwargs = {}) -> Kernel:
    return create('kernel', cwd, kwargs)
