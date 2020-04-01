from typing import Optional, Callable, Union, Any, List


class Task:
    """A function or command to be executed."""
    # function or shell script to be executed
    cmd: Optional[Union[str, Callable]] = None

    # arguments passed to cmd
    args: Optional[Union[List[tuple], tuple]] = None

    # number of MPI ranks (None if running in head node or determined by nnodes)
    nranks: Optional[int] = None

    # number of CPUs per MPI rank
    cpus_per_rank: int = 1

    # number of GPUs per MPI rank
    gpus_per_rank: int = 0

    # number of nodes (None if running in head node or determined by nranks)
    nnodes: Optional[int] = None

    # enable a GPU to be shared by multiple MPI processes
    mps: Optional[int] = None

    # name shown in output.log
    name: Optional[str] = None

    # working directory
    cwd: Optional[str] = None

    # optional function providing task progress
    probe: Optional[Callable] = None

    # result value of cmd
    result: Any = None

    def __init__(self, **kwargs):
        """Set properties."""
        for key, value in kwargs.items():
            setattr(self, key, value)
