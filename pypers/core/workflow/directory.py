from __future__ import annotations

import pickle
import toml
from os import path, fsync
from glob import glob
from subprocess import check_call
from typing import Any, List, Optional, Union, Callable, Literal, Iterable


class Directory:
    # path to root directory
    _cwd: str

    def __init__(self, cwd: str = '.'):
        self._cwd = path.normpath(cwd)
    
    def __eq__(self, d: Directory):
        return self.abs() == d.abs()
    
    def rel(self, *paths: str) -> str:
        """Get relative path of a sub directory."""
        return path.normpath(path.join(self._cwd, *paths))
    
    def abs(self, *paths: str) -> str:
        """Get absolute path of a sub directory."""
        return path.abspath(self.rel(*paths))
    
    def subdir(self, *paths: str) -> Directory:
        """Create a subdirectory object."""
        return Directory(self.rel(*paths))
    
    def has(self, src: str = '.'):
        """Check if a file or a directory exists."""
        return path.exists(self.rel(src))
    
    def rm(self, src: str = '.'):
        """Remove a file or a directory."""
        check_call('rm -rf ' + self.rel(src), shell=True)
    
    def cp(self, src: str, dst: str = '.'):
        """Copy file or a directory."""
        self.mkdir(path.dirname(dst))

        check_call(f'cp -r {self.rel(src)} {self.rel(dst)}', shell=True)
    
    def mv(self, src: str, dst: str = '.'):
        """Move a file or a directory."""
        self.mkdir(path.dirname(dst))

        check_call(f'mv {self.rel(src)} {self.rel(dst)}', shell=True)
    
    def ln(self, src: str, dst: str = '.'):
        """Link a file or a directory."""
        self.mkdir(path.dirname(dst))

        if not path.isabs(src):
            src = self.abs(src)

            if not path.isabs(dst):
                if not path.isdir(dstdir := self.abs(dst)):
                    dstdir = path.dirname(dstdir)

                src = path.join(path.relpath(path.dirname(src), dstdir), path.basename(src))

        check_call(f'ln -s {src} {self.rel(dst)}', shell=True)
    
    def mkdir(self, dst: str = '.'):
        """Create a directory recursively."""
        check_call('mkdir -p ' + self.rel(dst), shell=True)
    
    def ls(self, src: str = '.', grep: str = '*', isdir: Optional[bool] = None) -> List[str]:
        """List items in a directory."""
        entries: List[str] = []

        for entry in glob(self.rel(path.join(src, grep))):
            # skip non-directory entries
            if isdir is True and not path.isdir(entry):
                continue
            
            # skip directory entries
            if isdir is False and path.isdir(entry):
                continue
            
            entries.append(entry.split('/')[-1])

        return entries
    
    def lsdir(self, src: str = '.'):
        """Returns a list of subdirectories."""
        dirs = []

        for entry in self.ls(src, isdir=True):
            dirs.append(self.subdir(src, entry))
        
        return dirs

    def read(self, src: str) -> str:
        """Read text file."""
        with open(self.rel(src), 'r') as f:
            return f.read()

    def write(self, text: str, dst: str, mode: str = 'w'):
        """Write text and wait until write is complete."""
        self.mkdir(path.dirname(dst))

        with open(self.rel(dst), mode) as f:
            f.write(text)
            f.flush()
            fsync(f.fileno())
    
    def readlines(self, src: str) -> List[str]:
        """Read text file lines."""
        return self.read(src).split('\n')
    
    def writelines(self, lines: Iterable[str], dst: str, mode: str = 'w'):
        """Write text lines."""
        self.write('\n'.join(lines), dst, mode)
    
    def load(self, src: str, ext: Literal['pickle', 'toml', None] = None) -> Any:
        """Load a pickle / toml file."""
        if ext is None:
            ext = src.split('.')[-1] # type: ignore
        
        if ext == 'pickle':
            with open(self.rel(src), 'rb') as fb:
                return pickle.load(fb)
        
        elif ext == 'toml':
            with open(self.rel(src), 'r') as f:
                return toml.load(f)
        
        else:
            raise TypeError('Workspace.load() only supports pickle and toml')
    
    def dump(self, obj, dst: str, ext: Literal['pickle', 'toml', None] = None):
        """Save a pickle / toml file."""
        self.mkdir(path.dirname(dst))

        if ext is None:
            ext = dst.split('.')[-1] # type: ignore

        if ext == 'pickle':
            with open(self.rel(dst), 'wb') as fb:
                pickle.dump(obj, fb)
        
        elif ext == 'toml':
            with open(self.rel(dst), 'w') as f:
                toml.dump(obj, f)
        
        else:
            raise TypeError(f'Directory.dump() only supports pickle and toml ({dst})')

    async def mpiexec(self, cmd: Union[str, Callable], nprocs: int = 1,
        cpus_per_proc: int = 1, gpus_per_proc: int = 0, walltime: Optional[Union[float, str]] = None, resubmit: bool = False):
        from pypers.core.runtime.executor import mpiexec
        
        await mpiexec(self, cmd, nprocs, cpus_per_proc, gpus_per_proc, walltime, resubmit)
