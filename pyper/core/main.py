from __future__ import annotations

import sys
import pickle
import asyncio
from typing import Union, TYPE_CHECKING

from pyper.core.task import TaskRunner
from pyper.core.scheduler import Scheduler
from pyper.core.block import BlockRunner

if TYPE_CHECKING:
    from pyper.core.workflow import Workflow


async def _run(workflow: Workflow):
    """Execute workflow and periodically save checkpoint."""
    # clear unfinished status from previous run
    _clear(workflow.main)

    # periodic checkpoint
    asyncio.create_task(_worker(workflow))

    # main task
    scheduler = Scheduler(workflow.job)
    await scheduler.run(workflow.main)

    # save final state
    workflow.log()
    workflow.save()

def _clear(entry: Union[TaskRunner, BlockRunner]):
    """Clear the execution status from unfinished tasks."""
    if isinstance(entry, TaskRunner):
        if entry.exit_code != 0:
            entry.submit_time = None
            entry.start_time = None
            entry.end_time = None
            entry.exit_code = None
    
    elif isinstance(entry, BlockRunner):
        for child in entry.entries:
            _clear(child)
    
    else:
        raise TypeError(f'Unknown entry type: {entry}')

async def _worker(workflow: Workflow):
    """Periodically log and save checkpoint."""
    while True:
        try:
            workflow.log()
            workflow.save()

        except Exception as e:
            print(e, file=sys.stderr)

        await asyncio.sleep(workflow.job.checkpoint_interval)


if __name__ == '__main__':
    # load saved workflow
    with open('scratch/job.pickle', 'rb') as f:
        _workflow: Workflow = pickle.load(f)
    
    # execute workflow
    asyncio.run(_run(_workflow))
