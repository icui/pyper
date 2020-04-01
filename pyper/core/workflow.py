from __future__ import annotations

import sys
import pickle
from time import time
from datetime import timedelta
from typing import Union, TYPE_CHECKING

from pyper import shell, Task, Block
from pyper.core.task import TaskRunner
from pyper.core.block import BlockRunner

if TYPE_CHECKING:
    from pyper.core.job import Job


class Workflow:
    """Workflow manager."""
    # main workflow block
    main: Union[TaskRunner, BlockRunner]

    # job configuration
    job: Job

    def __init__(self, main: Block, job: Job):
        """Initialize."""
        if isinstance(main, Task):
            self.main = TaskRunner(main, job)
        
        elif isinstance(main, Block):
            self.main = BlockRunner(main, job)
        
        else:
            raise TypeError(f'Unknown entry type: {main}')

        self.job = job

    def log(self):
        """Save workflow state to output/output.log."""
        try:
            log = str(self.main)
        
        except Exception as e:
            print(e, file=sys.stderr)
        
        else:
            log += '\n\n'
            
            start_time = self.main.start_time
            end_time = self.main.end_time
            
            if start_time is None:
                log += 'pending\n'
            
            elif end_time is None:
                log += 'running'
                dt = int(round(time() - start_time))
                if dt:
                    log += ' (' + str(timedelta(seconds=dt)) + ')'
                
                log += '\n'
            
            elif self.main.exit_code:
                log += 'failed\n'
            
            else:
                log += 'done'
                dt = int(round(end_time - start_time))
                if dt:
                    log += ' (' + str(timedelta(seconds=dt)) + ')'
                
                log += '\n'
            
            shell.write('output/output.log', log)

    def save(self):
        """Save a checkpoint as pickle."""
        with open('scratch/job.pickle', 'wb') as f:
            pickle.dump(self, f)
