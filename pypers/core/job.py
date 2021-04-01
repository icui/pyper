from __future__ import annotations

from sys import stderr
from traceback import format_exc
from importlib import import_module
from typing import TYPE_CHECKING

from pypers.core.config import getarg, getcfg
from pypers.core.runtime.misc import cache
from pypers.core.workflow.directory import Directory

if TYPE_CHECKING:
    from pypers import Node


def load() -> Node:
    """Create or load main task based on config.toml."""
    from pypers import basedir as d

    if 'job' not in cache:
        if d.has('job.pickle'):
            cache['job'] = d.load('job.pickle')
        
        elif main := getcfg('job', 'main'):
            module = import_module(main[0])
            cache['job'] = getattr(module, main[1])()
        
        else:
            raise FileNotFoundError('job.pickle not exists')
    
    return cache['job']


def add_error(e):
    """Save error message.."""
    cwd, fid = getarg('mpiexec').split(':')
    
    print(cwd, fid, e, file=stderr)

    Directory(cwd).write(format_exc(), f'{fid}.error', 'a')
