from __future__ import annotations

from typing import Union, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyper import Task


class Block:
    """A group of tasks or blocks."""
    # execute tasks in parallel
    parallel: bool = False

    # name shown in output.log
    name: Optional[str] = None

    # list of tasks or blocks
    _entries: List[Union[Task, Block]]

    def __init__(self, **kwargs):
        """Set properties."""
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        self._entries = []

    def add(self, child: Union[Task, Block]) -> Union[Task, Block]:
        """Create and add a child task or block.
        
        Args:
            child (Union[Task, Block]): task or block to be added
        
        Returns:
            Union[Task, Block]: child
        """
        self._entries.append(child)
        return child
    
    def setup(self):
        """Initialization method added before job submission (intended for subclasses)."""
