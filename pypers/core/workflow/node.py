from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from os import chdir, path
from sys import argv
from typing import Optional, TYPE_CHECKING

from pypers.core.runtime import console
from pypers.core.runtime.misc import cache
from pypers.core.config import getarg, hasarg, getsys


if TYPE_CHECKING:
    from pypers import Workspace


class Node(ABC):
    """Base class of Workspace and Task, can be executed or submitted."""
    # parent node
    parent: Optional[Workspace] = None

    # exceptions occured during execution (not including InsuffifientTime)
    error: Optional[Exception]

    # exceptions occured during execution
    exception: Optional[Exception]

    # execution completed without exception
    done: bool

    # currently being executed
    running: bool

    # node name
    name: str

    @abstractmethod
    def reset(self):
        """Reset execution state."""
    
    @abstractmethod
    async def execute(self):
        """Execute self."""

    def __str__(self):
        name = self.name

        if state := self.state:
            name += f' ({state})'
        
        return name
    
    def _focus(self):
        """Set self as main job and save as job.pickle."""
        from pypers import basedir as d
        if 'job' in cache and cache['job'] is not self:
            raise RuntimeError(f'another job is running ({cache["job"]})')
        
        cache['job'] = self

        # check if argv demands switching to a child workspace
        if cwd := getarg('dir'):
            cwd = f'job.{cwd}'

            if d.has(cwd):
                raise FileExistsError(f'{cwd} exists')

        elif hasarg('n'):
            n = 0

            while d.has(cwd := f'job.{n:04d}'):
                n += 1

        # switch to child workspace
        if cwd:
            if d.has('config.toml'):
                # copy config.toml
                config = d.load('config.toml')

                # change job name
                if dirname := getarg('dir'):
                    if 'job' not in config:
                        config['job'] = {}
                    
                    config['job']['name'] = (config['job'].get('name') or '') + '_' + dirname

                # get entries from argv
                for arg in argv:
                    if '=' in arg and arg.startswith('--'):
                        key, val = arg.split('=')

                        if '.' in key:
                            keypair = key[2:].split('.')

                            if len(keypair) == 2:
                                section, key = keypair

                                if section in config and key in config[section]:
                                    oldval = config[section][key]
                                    
                                    if isinstance(oldval, bool):
                                        if val == 'true':
                                            config[section][key] = True
                                        
                                        elif val == 'false':
                                            config[section][key] = False
                                        
                                        else:
                                            raise TypeError(f'{val} is not a boolean ({section}.{key})')

                                    elif isinstance(oldval, int):
                                        config[section][key] = int(val)
                                    
                                    elif isinstance(oldval, float):
                                        config[section][key] = float(val)
                                    
                                    elif isinstance(oldval, str):
                                        config[section][key] = val
                                    
                                    else:
                                        raise TypeError(f'modifying {oldval} from CLI is not supported')
                
                # make paths absolute
                if 'path' in config:
                    for key, val in config['path'].items():
                        config['path'][key] = d.abs(val)
                
                d.dump(config, path.join(cwd, 'config.toml'))
                cache['config'] = config
            
            chdir(cwd)

        self.save()
    
    async def _block(self):
        """Block saving operation for 1 second after a successful save."""
        await asyncio.sleep(1)

        if 'saving' in cache and cache.pop('saving') == 2:
            self.save(False)
    
    def run(self):
        """Call self.execute() with asyncio."""
        self._focus()
        asyncio.run(self.execute())
        self.save()
        
    def submit(self):
        """Submit self.run() to job schedule."""
        self._focus()
        getsys('submit')(f'python -m "pypers.core.main" -r')
    
    def save(self, sync: bool = True):
        """Save base job to job.pickle if self is a descendant of base job."""
        from pypers import basedir as d
        
        if 'job' not in cache:
            console.error('no base workspace found')
            
        node = self

        while True:
            # save job only if it belongs to the main workflow
            if node is cache['job']:
                if sync:
                    # write immediately
                    d.dump(cache['job'], 'job.pickle')
                    
                elif 'saving' in cache:
                    # write after 1s
                    cache['saving'] = 2
                
                else:
                    # write and block writing for 1s
                    cache['saving'] = 1
                    d.dump(cache['job'], 'job.pickle')
                    asyncio.create_task(self._block())
                
                return

            if node.parent is None:
                console.error(f'{node} is not part of the base workspace')
                break

            node = node.parent

    @property
    def level(self):
        """Parent level count."""
        node = self
        level = 0

        while node.parent is not None:
            node = node.parent
            level += 1

        return level
    
    @property
    def state(self) -> Optional[str]:
        """Execution state."""
        if self.done:
            return 'done'
        
        elif self.running:
            return 'running'
        
        elif self.error:
            return 'failed'
        
        elif self.exception:
            return 'paused'
