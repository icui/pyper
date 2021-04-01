from functools import partial
from typing import Callable


def get_name(func: Callable) -> str:
    rawfunc = func

    # unwrap partial
    while isinstance(rawfunc, partial):
        if hasattr(rawfunc.func, '__name__') and rawfunc.func.__name__ == 'mpiexec' and len(rawfunc.args):
            # get name for wrapped mpiexec
            rawfunc = rawfunc.args[0]

            if not callable(rawfunc):
                return rawfunc.split('/')[-1].split()[0] if isinstance(rawfunc, str) else 'mpiexec'

        else:
            rawfunc = rawfunc.func
    
    if hasattr(rawfunc, '__name__'):
        return rawfunc.__name__.lstrip('_').rstrip('_')
    
    return ''
