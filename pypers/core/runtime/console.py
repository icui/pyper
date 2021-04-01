from __future__ import annotations

from sys import stdout, stderr
from typing import TYPE_CHECKING
import asyncio

from pypers.core.config import hasarg

if TYPE_CHECKING:
    from pypers import Task


# number of characters in current line
_nchars = 0

# tasks being monitored
_monitoring = []

# whether console.loop is running
_looping = False


async def _loop():
    """Periodically print node status."""
    global _looping

    while len(_monitoring):
        if len(_monitoring) > 1:
            for i, task in enumerate(_monitoring):
                stat('  ' * task.level + f'({i+1}/{len(_monitoring)}) {task}')
                await asyncio.sleep(2)
        
        else:
            stat('  ' * _monitoring[0].level + str(_monitoring[0]))
            await asyncio.sleep(2)

    _looping = False


def clear():
    """Clear current line."""
    global _nchars

    stdout.write('\r')

    for _ in range(_nchars):
        stdout.write(' ')
    
    stdout.write('\r')
    stdout.flush()

    _nchars = 0


def error(msg: str):
    """Show error message."""
    clear()
    print(msg, file=stderr)


def log(msg: str):
    """Show normal message."""
    clear()
    print(msg)


def stat(msg: str):
    """Display stat message in current line."""
    global _nchars
    
    clear()

    msg = msg.replace('\n', ' ')
    stdout.write('\r' + msg)
    stdout.flush()

    _nchars = len(msg)


def monitor(task: Task):
    """Monitor the execution status of task."""
    global _looping

    if task not in _monitoring:
        _monitoring.append(task)

    if not _looping and not hasarg('r'):
        _looping = True
        asyncio.create_task(_loop())


def unlink(task: Task):
    """Remove task from monitor list."""
    if task in _monitoring:
        _monitoring.remove(task)

        log('  ' * task.level + task.name)
