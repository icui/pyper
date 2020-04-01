#!/usr/bin/env python

from typing import cast

from pyper import Task, Block, submit
from _test import test

# counter of tasks
def counter_generator():
    """Count the number of added tasks."""
    num = 0

    while True:
        num += 1
        yield num

counter = counter_generator()

def test_tasks(target: Block):
    """Demonstrates how to add tasks."""
    # add a serial task to call a function
    target.add(Task(cmd=test, args=[next(counter)]))

    # add a parallel task to call a function
    target.add(Task(cmd=test, args=[next(counter)], nranks=3))

    # add a serial task to execute a shell command
    target.add(Task(cmd=f'./_test.sh {next(counter)}'))

    # add a parallel task to execute a shell command
    target.add(Task(cmd=f'./_test.sh {next(counter)}', nranks=2))

    # add a parallel GPU task to execute a shell command
    # specify gpus_per_rank (which should be 1 in most cases) to use GPU
    target.add(Task(cmd=f'./_test.sh {next(counter)}', nranks=2, gpus_per_rank=1))


def test_blocks(target: Block, add_blocks=False):
    """Demonstrates how to add blocks."""
    # create a serial child block
    child1 = cast(Block, target.add(Block()))

    # create a parallel child block
    child2 = cast(Block, target.add(Block(parallel=True)))

    # add tasks to child block
    test_tasks(child1)
    test_tasks(child2)

    # add blocks to block
    if add_blocks:
        test_blocks(child1)
        test_blocks(child2)

if __name__ == '__main__':
    """Test tasks and blocks."""
    # base workflow block (serial)
    main = Block()

    # add child tasks and child blocks
    test_tasks(main)
    test_blocks(main, True)

    # submit to job scheduler
    submit(main)
