from __future__ import annotations

import asyncio
from typing import Optional, Callable, Union, Dict, cast
from math import ceil
from time import time

from pypers.core.workflow.directory import Directory
from pypers.core.config import getsys, getcfg, hasarg
from pypers.core.runtime.walltime import maketime, checktime, InsufficientTime
from pypers.core.runtime.misc import ResubmitJob
from pypers.utils.func import get_name


# pending tasks
_pending: Dict[asyncio.Lock, int] = {}

# running tasks
_running: Dict[asyncio.Lock, int] = {}


def _dispatch(lock: asyncio.Lock, nnodes: int) -> bool:
    """Execute a task if resource is available."""
    ntotal: int = getcfg('job', 'nnodes')

    if nnodes > ntotal:
        raise RuntimeError(f'Insufficient nodes ({nnodes} / {ntotal})')

    if nnodes <= ntotal - sum(_running.values()):
        _running[lock] = nnodes
        return True
    
    return False


async def mpiexec(d: Directory, cmd: Union[str, Callable],
    nprocs: int, cpus_per_proc: int, gpus_per_proc: int, walltime: Optional[Union[float, str]], resubmit: bool = False):
    """Schedule the execution of MPI task"""
    # task queue controller
    lock = asyncio.Lock()

    # error occurred
    error = None

    # get walltime from config
    if isinstance(walltime, str):
        walltime = cast(Optional[float], getcfg('walltime', walltime))
    
    try:
        # calculate node number
        nnodes = int(ceil(nprocs * cpus_per_proc  / getsys('cpus_per_node')))

        if gpus_per_proc > 0:
            nnodes = max(nnodes, int(ceil(nprocs * gpus_per_proc  / getsys('gpus_per_node'))))

        # wait for node resources
        await lock.acquire()

        if not _dispatch(lock, nnodes):
            _pending[lock] = nnodes
            await lock.acquire()
        
        # make sure remaining time is enough
        if walltime:
            maketime(walltime)

        # save function as pickle to run in parallel
        if callable(cmd):
            funcname = get_name(cmd) + '\n'
            cwd = None
            fid = f'mpiexec.{id(cmd)}'
            d.rm(f'{fid}.*')
            d.dump(cmd, f'{fid}.pickle')
            cmd = f'python -m "pypers.core.main" --mpiexec={d.rel()}:{fid}'
        
        else:
            funcname = ''
            cwd = d.rel()
            fid = 'mpiexec'
        
        # wrap with parallel execution command
        cmd = getsys('mpiexec')(cmd, nprocs, cpus_per_proc, gpus_per_proc)
        
        # create subprocess to execute task
        with open(d.rel(f'{fid}.out'), 'a') as f:
            f.write(f'\n{funcname}{cmd}\n\n')

        with open(d.rel(f'{fid}.out'), 'a') as f:
            process = await asyncio.create_subprocess_shell(cast(str, cmd), cwd=cwd, stdout=f, stderr=f)
            time_start = time()
        
            if walltime and hasarg('r') and getcfg('job', 'requeue'):
                # abort when less than 1 minute remain
                try:
                    await asyncio.wait_for(process.communicate(), max(walltime, checktime() - 1) * 60)
                
                except asyncio.TimeoutError:
                    raise InsufficientTime(f'not enough time for {cmd}')
                
                except Exception as e:
                    raise e

                # save execution time history
                f.write(f'\nwalltime: {walltime}')
            
            else:
                await process.communicate()
            
            f.write(f'\nelapsed: {(time()-time_start)/60:.2f}\n')

        # catch error
        errcls = ResubmitJob if resubmit else RuntimeError

        if fid and d.has(f'{fid}.error'):
            raise errcls(d.read(f'{fid}.error'))

        if process.returncode:
            raise errcls(f'{cmd}\nexit code: {process.returncode}')
    
    except Exception as e:
        error = e
    
    # clear entry
    if lock in _pending:
        del _pending[lock]
    
    if lock in _running:
        del _running[lock]
    
    # sort entries by their node number
    pendings = sorted(_pending.items(), key=lambda item: item[1], reverse=True)

    # execute tasks if resource is available
    for lock, nnodes in pendings:
        if _dispatch(lock, nnodes):
            del _pending[lock]
            lock.release()

    if error:
        raise error
