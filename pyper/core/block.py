from __future__ import annotations

from datetime import timedelta
from typing import List, Union, Optional, TYPE_CHECKING

from pyper import Task, Block
from pyper.core.task import TaskRunner

if TYPE_CHECKING:
    from pyper.core.job import Job


class BlockRunner:
    """Wrapped block for execution."""
    # name shown in output.log
    name: str

    # execute tasks in parallel
    parallel: bool

    # list of tasks or blocks
    entries: List[Union[TaskRunner, BlockRunner]]

    @property
    def submit_time(self) -> Optional[float]:
        """Earliest schedule time from child entries."""
        times = list(child.submit_time for child in self.entries if child.submit_time is not None)
        return min(times) if len(times) > 0 else None

    @property
    def start_time(self) -> Optional[float]:
        """Earliest start time from child entries."""
        times = list(child.start_time for child in self.entries if child.start_time is not None)
        return min(times) if len(times) > 0 else None

    @property
    def end_time(self) -> Optional[float]:
        """Latest end time from child entries."""
        times = list(child.end_time for child in self.entries)
        return max(times) if (len(times) > 0 and all(times)) else None

    @property
    def exit_code(self) -> Optional[int]:
        """None if any task is not finished, 1 if any task failed, else 0."""
        codes = list(child.exit_code for child in self.entries)

        if len(codes) == 0 or any(code is None for code in codes):
            return None
        
        return 1 if any(codes) else 0

    def __init__(self, block: Block, job: Job):
        # call setup method
        block.setup()
        
        # fill block name
        if block.name is None:
            if block.__class__.__name__ != 'Block':
                self.name = block.__class__.__name__.lower()
            
            else:
                self.name = ''
        
        else:
            self.name = block.name

        # parallel mode
        self.parallel = block.parallel

        # child entries
        self.entries = []

        for entry in block._entries:
            if isinstance(entry, Task):
                self.entries.append(TaskRunner(entry, job))
            
            elif isinstance(entry, Block):
                self.entries.append(BlockRunner(entry, job))
            
            else:
                raise TypeError(f'Unexpected entry type {entry}.')

    def __str__(self) -> str:
        """Get the log string of a block."""
        block_str = self.name
        start_time = self.start_time
        end_time = self.end_time

        if start_time and end_time:
            dt = int(round(end_time - start_time))
            if dt:
                block_str += ' (' + str(timedelta(seconds=dt)) + ')'

        for i in range(len(self.entries)):
            # log string of child entry
            block_str += '\n   '
        
            # list symbol
            if self.parallel:
                block_str += '-'

            else:
                block_str += '0' * (len(str(len(self.entries))) - len(str(i + 1))) + str(i + 1) + ')'
            
            # increase indent for child str
            block_str += ' ' + str(self.entries[i]).replace('\n', '\n  ')

        return block_str
