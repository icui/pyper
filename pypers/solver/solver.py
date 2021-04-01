from __future__ import annotations

from typing import Optional, Union, List

from pypers import field, Workspace
from pypers.fwi.workspace import create, FWIKwargs, ASDFKwargs


class Solver(Workspace):
    # current iteration
    iteration: int = field(0)

    # path to CMTSOLUTION
    path_event: Optional[str] = field()

    # path to STATIONS
    path_stations: Optional[str] = field()

    # path to model
    path_model: Optional[str] = field()

    # path to adjoint source
    path_adjoint: Optional[str] = field()

    # path to forward simulation directory
    path_forward: Optional[str] = field()

    # simulation duration
    duration: Optional[float] = field()

    # transient state duration for source encoding
    transient_duration: Optional[float] = field()

    # use monochromatic source
    monochromatic_source: bool = field(False)

    # save snapshots of forward wavefield
    save_forward: bool = field(False)

    # radius for smoothing kernels
    smooth_kernels: Optional[Union[float, List[float]]] = field()

    # radius for smoothing hessian
    smooth_hessian: Optional[Union[float, List[float]]] = field()

    # precondition threshold for kernels
    precondition: Optional[float] = field()

    # process output trace
    process_traces: Optional[ASDFKwargs] = field()


def create_solver(cwd: str = '.', kwargs: FWIKwargs = {}) -> Solver:
    return create('solver', cwd, kwargs)
