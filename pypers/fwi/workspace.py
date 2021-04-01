from __future__ import annotations

from importlib import import_module
from typing import TypedDict, List, Optional, Union, Literal, TYPE_CHECKING

from pypers import getcfg

if TYPE_CHECKING:
    from asdfy import ASDFFunction


class ASDFKwargs(TypedDict, total=False):
    """Arguments for processing ASDF traces."""
    # processing function
    func: Optional[ASDFFunction]

    # output file
    dst: Optional[str]

    # type of input data for func
    input_type: Literal['stream', 'trace', 'auxiliary']

    # tag of ASDF input data
    input_tag: Optional[str]

    # tag of ASDF output data
    output_tag: Optional[str]

    # pass ASDFAccessor to func
    accessor: bool

    # number of processors to use
    nprocs: Optional[int]

    # name of the processing task
    name: str


class FWIKwargs(TypedDict, total=False):
    """Arguments for creating solver workspace."""
    # path to event file
    path_event: Optional[str]

    # path to station file(s)
    path_stations: Optional[str]

    # path to model file
    path_model: Optional[str]

    # path to adjoint sources
    path_adjoint: Optional[str]

    # path to forward workspace
    path_forward: Optional[str]

    # path to observed traces
    path_observed: str

    # path to synthetic traces
    path_synthetic: str

    # path to encoded observed traces
    path_encoded: str

    # simulation duration in minutes
    duration: Optional[float]

    # transient state simulation duration in minutes
    transient_duration: Optional[float]

    # use monochromatic source time function
    monochromatic_source: bool

    # save forward wavefield for adjoint simulation
    save_forward: bool
    
    # radius for smoothing kernels
    smooth_kernels: Optional[Union[float, List[float]]]

    # radius for smoothing hessian
    smooth_hessian: Optional[Union[float, List[float]]]

    # threshold for applying preconditioner
    precondition: Optional[float]

    # arguments for processing output traces
    process_traces: Optional[ASDFKwargs]

    # lendth of tapering traces in minutes
    taper: Optional[float]

    # skip kernel computation
    misfit_only: bool

    # use double difference misfit
    double_difference: bool

    # period range used
    period_range: List[float]

    # number of iterations
    niters: int

    # maximum number of search steps
    nsteps: int


def create(section: str, cwd: str, kwargs: FWIKwargs):
    """Load module based on config.toml."""
    use = getcfg('module', section)
    module = import_module(f'pypers.{section}.{use}')

    # find space constructor and create space
    for clsname in dir(module):
        if clsname.lower() == use:
            target = getattr(module, clsname)(cwd, kwargs)
            target.add(target.setup)
            return target
    
    raise ValueError(f'no `{section}` module found')
