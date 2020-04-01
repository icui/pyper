import asyncio
import sys
import traceback
import pickle
from time import time
from subprocess import call
from typing import List, Dict, Union

from pyper import config, shell
from pyper.core.job import Job
from pyper.core.task import TaskRunner
from pyper.core.block import BlockRunner


class Scheduler:
    """Schedule the running of parallel jobs."""
    # job configuration
    job: Job

    # tasks waiting for execution
    _pending: Dict[TaskRunner, asyncio.Lock]

    # tasks currently being executed
    _running: List[TaskRunner]

    def __init__(self, job: Job):
        """Initialize."""
        self.job = job
        self._pending = {}
        self._running = []

    async def run(self, entry: Union[TaskRunner, BlockRunner]):
        """Schedule a task for execution.
        
        Args:
            task (Task): task to be scheduled
        """
        if isinstance(entry, TaskRunner):
            await self._run_task(entry)
        
        elif isinstance(entry, BlockRunner):
            await self._run_block(entry)
        
        else:
            raise RuntimeError(f'Unsupported scheduler entry ({entry})')
    
    async def _run_task(self, task: TaskRunner):
        """Schedule a task for execution.
        
        Args:
            task (Task): task to be scheduled
        """
        # reset state
        if task.exit_code == 0:
            return
        
        task.submit_time = time()

        # wait for scheduling for tasks that run on compute node
        if task.mpi:
            # create to lock which is released when task is allocated for execution
            lock = asyncio.Lock()
            await lock.acquire()

            # schedule task
            self._pending[task] = lock
            self._update()

            # wait until task is dispatched
            await lock.acquire()
        
        # mark as started
        task.start_time = time()

        # execute task
        try:
            cmd = task.cmd
            prefix = f'scratch/tasks/{id(cmd)}'
            shell.rm(f'{prefix}.*')

            if isinstance(cmd, str) and len(task.args) > 0:
                cmd += ' ' + ' '.join(str(arg) for arg in task.args)

            if cmd is None:
                # empty task
                pass

            elif task.mpi is not None:
                # task running in compute node
                if callable(cmd):
                    # save function as pickle to run in parallel
                    with open(f'{prefix}.pickle', 'wb') as f:
                        pickle.dump((cmd, task.args), f)
                    
                    cmd = f'python -m "pyper.core.dispatch" {prefix}'

                # wrap with parallel execution command
                cmd = config.get_module('pyper.cluster').mpiexec(cmd, *task.mpi)
                shell.write('output/shell.log', f'> {cmd}\n', 'a')
                
                # create subprocess to execute task
                process = await asyncio.create_subprocess_shell(cmd, cwd=task.cwd)
                await process.communicate()

                # finalize
                task.exit_code = process.returncode

            else:
                # task running in head node
                if callable(cmd):
                    task.task.result = cmd(*task.args)

                else:
                    task.exit_code = call(cmd, cwd=task.cwd, shell=True)

                    if task.exit_code:
                        print(f'Task `{task.cmd}` returned non-zero exit code.')
            
            # get result
            if shell.exists(f'{prefix}.err'):
                task.exit_code = -1
            
            elif not task.exit_code and shell.exists(f'{prefix}.result.pickle'):
                with open(f'{prefix}.result.pickle', 'rb') as f:
                    task.task.result = pickle.load(f)

        except Exception:
            traceback.print_exc(file=sys.stderr)
            task.exit_code = 1
        
        else:
            if task.exit_code is None:
                task.exit_code = 0
        
        # mark as finished
        task.end_time = time()

        # schedule next task
        if task.mpi:
            self._running.remove(task)
            self._update()

    async def _run_block(self, block: BlockRunner):
        """Execute a block.
        
        Args:
            block (Block): block to be executed
        """
        if block.parallel:
            # execute block in parallel
            await asyncio.gather(*(self.run(child) for child in block.entries))

        else:
            # execute block in serial
            for child in block.entries:
                # await child execution
                await self.run(child)

                # exit if step failed
                if child.exit_code != 0:
                    break

    def _update(self):
        """Try to schedule tasks when a task is scheduled or released."""
        # number of available nodes
        nnodes = self.job.nnodes

        # remove finished or failed tasks and get available nodes
        for task in self._running:
            nnodes -= task.mpi[1]

        # sort entries by their node number
        tasks: List[TaskRunner] = sorted(self._pending.keys(), key=lambda task: task.mpi[1], reverse=True)

        # execute tasks if resource is available
        for task in tasks:
            if task.mpi[1] <= nnodes:
                # allocate resource
                nnodes -= task.mpi[1]
                self._running.append(task)

                # notify executor
                lock = self._pending.pop(task)
                lock.release()
