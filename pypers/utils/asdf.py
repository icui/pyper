from __future__ import annotations

from typing import Union, Iterable, Literal, Optional, TYPE_CHECKING
from functools import partial

from pypers import Directory, Task, getsys

if TYPE_CHECKING:
    from asdfy import ASDFFunction, ASDFProcessor


def _probe(dst: str):
    """Check the processing status."""
    from os.path import exists, getsize

    if dst and exists(dst):
        size = getsize(dst) >> 20

        if size > 1000:
            return f'{size/1000:.2f}GB written'
        
        elif size > 0:
            return f'{size}MB written'
        
    return 'processing'


async def _run(ap: ASDFProcessor, nprocs: int, walltime: Optional[Union[float, str]]):
    """Execute ASDFProcessor."""
    task: Task = getattr(ap, '_task')
    d = Directory(task.parent.rel() if task.parent else '.')

    await d.mpiexec(ap.run, nprocs, walltime=walltime)


def asdf_task(src: Union[str, Union[str, Iterable[str]]], dst: Optional[str] = None,
    func: Optional[ASDFFunction] = None, input_type: Literal['stream', 'trace', 'auxiliary', 'auxiliary_group'] = 'trace',
    input_tag: Optional[str] = None, output_tag: Optional[str] = None, accessor: bool = False, pairwise: bool = False,
    nprocs: Optional[int] = None, name: str = 'process_traces', walltime: Optional[Union[float, str]] = 'process_traces') -> Task:
    """Create a task that processes ASDF."""
    from asdfy import ASDFProcessor
    from pypers.core.job import add_error

    # default number of processors
    nprocs = nprocs or getsys('cpus_per_node')

    # create processor object
    ap = ASDFProcessor(src, dst, func, input_type, input_tag, output_tag, accessor, pairwise, add_error)
    task = Task(partial(_run, ap, nprocs, walltime), name, partial(_probe, dst))

    # save task reference to determine cwd
    setattr(ap, '_task', task)
    
    return task
