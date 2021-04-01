from sys import stderr
from typing import Optional, Union
from time import time

from pypers.core.config import getcfg, hasarg
from pypers.core.runtime import console
from pypers.core.runtime.misc import ResubmitJob


# start time
_starttime = time()


class InsufficientTime(Exception):
    """Exception due to insufficient execution time."""


def checktime():
    """Get remaining walltime in minutes."""
    return getcfg('job', 'walltime') - (time() - _starttime) / 60


def maketime(walltime: Optional[Union[float, str]]):
    """Ensure that remaining time is more than walltime."""
    remaining = checktime()

    if isinstance(walltime, str):
        wt = getcfg('walltime', walltime)

        if wt is None:
            console.error(f'warning: walltime `{walltime}` is not defined')
        
        walltime = wt

    if walltime and walltime >= remaining:
        msg = f'Insufficient execution time ({walltime:.2f}min / {remaining:.2f}min)'

        if hasarg('r') and getcfg('job', 'requeue'):
            raise InsufficientTime(msg)
        
        else:
            print(msg, file=stderr)
