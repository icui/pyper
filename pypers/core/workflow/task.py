from time import time
from asyncio import iscoroutine
from typing import Callable, Optional, Union
from traceback import format_exc

from pypers.utils.func import get_name
from pypers.core.runtime import console
from pypers.core.runtime.misc import ResubmitJob

from .node import Node


class Task(Node):
    """A wrapper of function call."""
    # task display name
    _name: str

    # wrapped function
    _func: Callable

    # function to check status
    _prober: Optional[Callable[..., Union[str, float]]]

    # time when function is called
    _starttime: Optional[float] = None

    # time when function ends
    _endtime: Optional[float] = None

    # exception during function execution
    _exception: Optional[Exception] = None

    def __init__(self, func: Callable, name: Optional[str] = None, prober: Optional[Callable] = None):
        self._func = func
        self._name = name or get_name(func)
        self._prober = prober
    
    def reset(self):
        """Clear execution state."""
        self._starttime = None
        self._endtime = None
        self._exception = None

    async def execute(self):
        """Call function."""
        console.monitor(self)
        
        # initialize
        self.reset()
        self._starttime = time()
        self.save(False)
        
        try:
            if iscoroutine(result := self._func()):
                await result

            self._endtime = time()
        
        except Exception as e:
            self._exception = e

            if isinstance(e, ResubmitJob):
                console.log(e.args[0])
            
            else:
                console.error(format_exc())
        
        self.save(False)

        console.unlink(self)
    
    @property
    def error(self) -> Optional[Exception]:
        """Has error that is not ResubmitJob."""
        if isinstance(self._exception, ResubmitJob):
            return None
        
        return self._exception

    @property
    def exception(self) -> Optional[Exception]:
        """Exception occurred during execution."""
        return self._exception

    @property
    def done(self) -> bool:
        """Task exited successfully."""
        return self._starttime is not None and self._endtime is not None and self._exception is None
    
    @property
    def running(self) -> bool:
        """If task is currently running."""
        return self._starttime is not None and self._endtime is None and self._exception is None
    
    @property
    def name(self) -> str:
        """Task name."""
        return self._name
    
    @property
    def state(self) -> Optional[str]:
        """Execution state."""
        if self.running and self._prober:
            try:
                progress = self._prober()
            
            except:
                pass
            
            else:
                if isinstance(progress, float) and 0 <= progress <= 1:
                    return f'{int(progress * 100)}%'

                elif isinstance(progress, str):
                    return progress
            
        return super().state
