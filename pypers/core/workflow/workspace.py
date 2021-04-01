from __future__ import annotations

import asyncio
from typing import Optional, List, Union, Callable, Any
from collections import namedtuple

from pypers.core.config import getcfg, hasarg
from pypers.core.runtime import console
from pypers.core.runtime.misc import cache

from .task import Task
from .node import Node
from .directory import Directory


# getter of workspace properties
Field = namedtuple('Field', ('default', 'required'))


def field(default: Any = None, required: bool = False) -> Any:
    return Field(default, required)


class Workspace(Directory, Node):
    """A wrapper of Directory to execute tasks."""
    # execute task concurrently
    _concurrent: bool

    # child spaces or tasks
    _nodes: List[Node]

    # initial properties
    _kwargs: dict

    # assigned properties
    _dict: dict

    def __init__(self, cwd: str = '.', kwargs: Optional[dict] = None, concurrent: bool = False):
        super().__init__(cwd)

        self._nodes = []
        self._concurrent = concurrent
        self._kwargs = kwargs or {}
        self._dict = {}
    
    def __len__(self):
        return len(self._nodes)
    
    def __iter__(self):
        return iter(self._nodes)
    
    def __getitem__(self, key: Union[str, int]):
        if isinstance(key, int):
            return self._nodes[key]
        
        if key in self._dict:
            return self._dict[key]
        
        if key in self._kwargs:
            return self._kwargs[key]
        
        if self.parent:
            return self.parent[key]
        
        if (val := getcfg('workspace', key)) is not None:
            return val
        
        return None
    
    def __setitem__(self, key: str, val):
        """Set state in current directory."""
        self._dict[key] = val
    
    def __delitem__(self, key: str):
        """Delete a state."""
        del self._dict[key]
    
    def __contains__(self, key):
        if key in self._dict or key in self._kwargs or getcfg('workspace', key) is not None:
            return True
        
        if self.parent:
            return key in self.parent
        
        return False
    
    def __getattribute__(self, key: str):
        val = super().__getattribute__(key)

        if isinstance(val, Field):
            if key in self:
                return self[key]
            
            if val.required:
                raise TypeError(f'required field {key} for <{self}> is missing')
            
            return val.default
        
        return val
    
    def __setattr__(self, key: str, val: Any):
        if hasattr(self, key) and isinstance(super().__getattribute__(key), Field):
            self[key] = val

        else:
            super().__setattr__(key, val)
    
    def _get_unfinished(self, exclude: List[Node] = []):
        """Get nodes that are not finished."""
        nodes = []

        for node in self._nodes:
            if node not in exclude and not node.done:
                nodes.append(node)
        
        return nodes
    
    def rel(self, *paths: str) -> str:
        """Get relative path of a sub directory."""
        if self.parent:
            return self.parent.rel(self._cwd, *paths)
        
        return super().rel(*paths)
    
    def add(self, node: Union[Node, Callable], name: Optional[str] = None, prober: Optional[Callable] = None):
        """Add a child Workspace or task."""
        if callable(node):
            node = Task(node, name, prober)

        if node.parent:
            raise RuntimeError(f'{node} being added to multiple places')

        node.parent = self

        self._nodes.append(node)
    
    def clear(self, keep_first: bool = True):
        """Delete all child nodes (except the first node)."""
        if keep_first:
            del self._nodes[1:]
        
        else:
            self._nodes.clear()
        
        self._dict.clear()

        if self.rel() != '.':
            self.rm()
            self.mkdir()
    
    def reset(self):
        """Reset task."""
        self.rewind(len(self))
    
    def rewind(self, n: int = 1):
        """Reset the last n finished child node(s)."""
        for _ in range(n):
            for i in range(len(self) - 1, -1, -1):
                node = self._nodes[i]

                if node.done or node.running or node.exception:
                    node.reset()
                    break
                
                if i == 0:
                    return

    async def execute(self):
        """Execute all nodes."""
        console.log('  ' * self.level + self.name)
        
        # skip executed nodes
        exclude = []

        while len(nodes := self._get_unfinished(exclude)):
            if self._concurrent:
                # execute nodes concurrently
                await asyncio.gather(*(node.execute() for node in nodes))
                exclude += nodes

            else:
                # execute nodes in sequence
                node = nodes[0]
                await node.execute()

                if node.exception:
                    break
        
    @property
    def error(self) -> Optional[Exception]:
        """Errors in child nodes other than ResubmitJob."""
        errors = []

        for node in self._get_unfinished():
            if node.error:
                errors.append(node.error)

        if len(errors):
            return RuntimeError(f'{len(errors)} errors.')
        
        return None

    @property
    def exception(self) -> Optional[Exception]:
        """Errors in self or child nodes."""
        exceptions = []

        for node in self._get_unfinished():
            if node.exception:
                exceptions.append(node.exception)

        if len(exceptions):
            return Exception(f'{len(exceptions)} exceptions.')
        
        return None
    
    @property
    def done(self) -> bool:
        """All nodes exited successfully."""
        return len(self._get_unfinished()) == 0
    
    @property
    def running(self) -> bool:
        """If any child node is running."""
        for node in self._get_unfinished():
            if node.running:
                return True
        
        return False
    
    @property
    def name(self) -> str:
        """Name of the workspace."""
        return getcfg('job', 'name') if self.rel() == '.' else self.rel().split('/')[-1]
    
    @property
    def info(self):
        """Structure and execution status."""
        stat = str(self)
        verbose = hasarg('v')

        if not verbose:
            stat = stat.split(' ')[0]

        def idx(j):
            if self._concurrent:
                return '- '

            return '0' * (len(str(len(self) + 1)) - len(str(j + 1))) + str(j + 1) + ') '
            
        collapsed = False

        for i, node in enumerate(self):
            stat += '\n' + idx(i)

            if not verbose and (node.done or (collapsed and not node.running and not node.exception)):
                stat += str(node)
        
            else:
                collapsed = True
                
                if isinstance(node, Workspace):
                    stat += '\n  '.join(node.info.split('\n'))
                
                else:
                    stat += str(node)
        
        return stat
