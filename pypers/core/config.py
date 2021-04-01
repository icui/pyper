from sys import argv
from os import path
from typing import Optional, Any
from importlib import import_module
import toml

from pypers.core.runtime.misc import cache


def getarg(key: str) -> Optional[str]:
    """Get named value from sys.argv."""
    for arg in argv[1:]:
        if arg.startswith(f'--{key}='):
            return arg.split('=')[1]
    
    return None


def hasarg(key: str) -> bool:
    """Check if sys.argv has a specific argument."""
    return f'-{key}' in argv or getarg(key) is not None


def getcfg(section: str, key: str) -> Any:
    """Get a copy of a config entry."""
    # load config.toml
    if 'config' not in cache:
        if not path.exists('config.toml'):
            return None

        with open('config.toml', 'r') as f:
            cache['config'] = toml.load(f)

    if section in cache['config'] and key in cache['config'][section]:
        return cache['config'][section][key]

    return None

def getsys(key: str):
    """Get the configuration of selected cluster."""
    if key in ('cpus_per_node', 'gpus_per_node') and (val := getcfg('job', key)) is not None:
        return val
    
    cluster = getcfg('job', 'cluster')

    return getattr(import_module(f'pypers.cluster.{cluster}'), key)


def getpath(name: str, *paths: str):
    """Get path from config.toml."""
    if (src := getcfg('path', name)) is not None:
        return path.abspath(path.join(src, *paths))
    
    return ''
