from __future__ import annotations

import sys
from math import ceil
from time import time
from datetime import timedelta
from typing import Optional, Callable, Union, List, Tuple, cast, TYPE_CHECKING


if TYPE_CHECKING:
    from pyper import Task
    from pyper.core.job import Job


class TaskRunner:
    """Wrapped task for execution."""
    # name shown in output.log
    name: str

    # function or shell script to be executed
    cmd: Optional[Union[str, Callable]]

    # working directory
    cwd: Optional[str]

    # arguments passed to cmd
    args: Union[List[tuple], tuple]

    # mpi configurations (nranks, nnodes, cpus, gpus)
    mpi: Optional[Tuple[int, int, int, int]]

    # optional function providing task progress
    probe: Optional[Callable]

    # time when scheduled for running
    submit_time: Optional[float] = None

    # time when task start running
    start_time: Optional[float] = None

    # time shen task end running
    end_time: Optional[float] = None

    # exit code of task
    exit_code: Optional[int] = None

    # original task
    task: Task

    def __init__(self, task: Task, job: Job):
        """Fill missing properties and verify."""
        # fill configurations for tasks running in compute node
        if task.nranks is not None or task.nnodes is not None:
            self._fill_mpi(task, job)
        
        else:
            self.mpi = None
        
        # cmd and arguments
        self._fill_cmd(task)

        # fill task name
        if task.name is None:
            self._fill_name(task)
        
        else:
            self.name = task.name
        
        # link original task for saving result
        self.task = task

    def __str__(self) -> str:
        """Get the log string of a task."""
        task_str = self.name

        if self.end_time is not None:
            if self.exit_code:
                # mark failed
                task_str += ' failed'
            
            else:
                # add elapsed time to finished task
                task_str += ' done'
                dt = int(round(self.end_time - cast(float, self.start_time)))
                
                if dt:
                    task_str += f' ({str(timedelta(seconds=dt))})'
        
        elif self.start_time is not None:
            # add progress to running task
            task_str += ' running (' + str(timedelta(seconds=int(round(time() - self.start_time))))

            if callable(self.probe):
                try:
                    probe = self.probe()
                    task_str += ' - ' + str(int(probe * 100)) + '%'

                except Exception as e:
                    print(e, file=sys.stderr)
            
            task_str += ')'
        
        elif self.submit_time is not None:
            task_str += ' pending'

        return task_str
    
    def _fill_mpi(self, task: Task, job: Job):
        """Fill and check MPI configurations."""
        # task configuration
        nranks = task.nranks
        nnodes = task.nnodes

        # check rank and node configuration
        if nranks is not None and (nranks <= 0 or not isinstance(nranks, int)):
            raise ValueError('Number of MPI ranks must be a positive integer.')
        
        if nnodes is not None and (nnodes <= 0 or not isinstance(nnodes, int)):
            raise ValueError('Number of nodes must be a positive integer.')

        # check cpu configuration
        if task.cpus_per_rank <= 0 or not isinstance(task.cpus_per_rank, int):
            raise ValueError('Number of CPUs must be a positive integer.')

        # check gpu configuration
        if task.gpus_per_rank < 0 or not isinstance(task.gpus_per_rank, int):
            raise ValueError('Number of GPUs must be a non-negative integer.')

        # get the maximium number of MPI ranks per node
        nranks_per_node = job.cpus_per_node // task.cpus_per_rank

        if task.gpus_per_rank:
            # make sure GPU is enabled
            if job.gpus_per_node == 0:
                raise ValueError('Unable to run a GPU task on a CPU cluster')

            # get number of GPU MPI ranks per node
            if task.mps:
                if not isinstance(task.mps, int) or task.mps <= 1:
                    raise ValueError('MPS must be a integer that is greater than 1.')
                
                if task.gpus_per_rank != 1:
                    raise ValueError('Number of GPUs per rank must be 1 if MPS is enabled.')

                nranks_per_node_gpu = task.mps * job.gpus_per_node

            else:
                nranks_per_node_gpu = job.gpus_per_node // task.gpus_per_rank

            nranks_per_node = min(nranks_per_node, nranks_per_node_gpu)

        # fill nranks or nnodes
        if nranks is None:
            # total number of MPI ranks provided by nnodes
            nranks = nranks_per_node * cast(int, nnodes)

        if nnodes is None:
            # round up to get the number of nodes needed to provide nranks
            nnodes = int(ceil(nranks / nranks_per_node))
        
        # make sure nnodes provide enough MPI ranks
        if nranks > nranks_per_node * nnodes:
            raise ValueError(f'Too many MPI ranks ({nranks}).')

        # make sure requested node number is enough
        if nnodes > job.nnodes:
            raise ValueError(f'Too few nodes requested ({nnodes} / {job.nnodes}).')
        
        # total number of cpus and gpus
        cpus = nranks * task.cpus_per_rank
        gpus = nranks * task.gpus_per_rank

        if task.mps:
            if gpus % task.mps != 0:
                raise ValueError('Number of ranks must be a multiple of MPS ({gpus} / {task.mps}).')

            gpus //= task.mps
        
        self.mpi = (nranks, nnodes, cpus, gpus)
    
    def _fill_cmd(self, task: Task):
        """Fill and check command."""
        if task.cwd and callable(task.cmd):
            raise RuntimeError('`cwd` argument is only available to tasks that call shell commands.')

        self.cmd = task.cmd
        self.cwd = task.cwd

        if task.args is not None:
            if isinstance(task.args, list):
                if not self.mpi or not callable(self.cmd):
                    raise RuntimeError(f'List type arguments can only be passed to MPI tasks {task}.')
                
                if self.mpi[0] != len(task.args):
                    raise RuntimeError('Length of arguments must equal to the number of MPI tasks.')
                
                for arg in task.args:
                    if not isinstance(arg, tuple):
                        raise TypeError(f'Arguments can only be type tuple {task.args}.')
            
            elif not isinstance(task.args, tuple):
                raise TypeError(f'Arguments can only be type tuple {task.args}.')
            
            self.args = task.args
        
        else:
            self.args = ()
        
        # function to check progress
        self.probe = task.probe

    def _fill_name(self, task: Task):
        """Fill task name."""
        if task.__class__.__name__ != 'Task':
            self.name = task.__class__.__name__.lower()
        
        elif callable(self.cmd):
            if hasattr(self.cmd, '__name__'):
                if self.cmd.__name__ != 'cmd':
                    self.name = self.cmd.__name__

            elif hasattr(self.cmd, 'func'):
                func = getattr(self.cmd, 'func')
                if hasattr(func, '__name__'):
                    self.name = func.__name__

        elif isinstance(self.cmd, str):
            self.name = self.cmd.split(' ')[0].split('/')[-1]

        else:
            self.name = ''
