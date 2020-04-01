#!/usr/bin/env python

from pyper import Task, Block, submit
from _test import test

# counter of tasks
def _counter_generator():
    """Count the number of added tasks."""
    num = 0

    while True:
        num += 1
        yield num

_counter = _counter_generator()

def _test_tasks(target: Block):
    """Demonstrates how to add tasks."""
    # add a serial task to call a function
    # for serial tasks, args (arguments passed to function) should have type tuple
    target.add(Task(cmd=test, args=(next(_counter),)))

    # add a parallel task to call a function
    # for parallel tasks, arguments passed to function may have type list or type tuple
    # if args is tuple, then the same args is used across all ranks
    # if args is list, then it should have a length equal to nranks, each element should be a tuple
    # specify gpus_per_rank (which should be 0 or 1 in most cases) to use GPU
    target.add(Task(cmd=test, args=(next(_counter),), nranks=3, gpus_per_rank=1))
    target.add(Task(cmd=test, args=[(next(_counter),), (next(_counter),), (next(_counter),)], nranks=3))

    # add a serial task to execute a shell command
    target.add(Task(cmd=f'sh _test.sh', args=(next(_counter),)))

    # add a parallel task to execute a shell command
    target.add(Task(cmd=f'sh _test.sh', args=(next(_counter),), nranks=2))

    # add a parallel GPU task to execute a shell command
    target.add(Task(cmd=f'sh _test.sh', args=(next(_counter),), nranks=2, gpus_per_rank=1))


def _test_blocks(target: Block, add_blocks=False):
    """Demonstrates how to add blocks."""
    # create a block that runs child tasks / blocks in serial
    child1 = Block()
    target.add(child1)

    # create a block that runs child tasks / blocks in parallel
    child2 = Block(parallel=True)
    target.add(child2)

    # add tasks to child block
    _test_tasks(child1)
    _test_tasks(child2)

    # add blocks to block
    if add_blocks:
        _test_blocks(child1)
        _test_blocks(child2)

if __name__ == '__main__':
    """Test tasks and blocks."""
    # base workflow block (serial)
    main = Block()

    # add child tasks and child blocks
    _test_tasks(main)
    _test_blocks(main, True)

    # submit to job scheduler
    submit(main)
    print('Check out output/output.log to monitor job status.')
