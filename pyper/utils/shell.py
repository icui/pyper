from os import path, fsync, getcwd
from typing import List, Optional
from glob import glob
from subprocess import check_call


class Shell:
    """OS operations."""
    # working directory
    _cwd: str

    # directories that can write to without sudo argument
    _dirs: List[str] = ['scratch', 'output']

    @property
    def cwd(self):
        """Current working directory."""
        return self._cwd

    def __init__(self, cwd: Optional[str] = None):
        """Set working directory."""
        self._cwd = getcwd() if cwd is None else cwd

    def abspath(self, src: str) -> str:
        """Get absolute path.

        Args:
            src (str): relative path

        Returns:
            str: absolute path
        """
        if src.startswith('/'):
            return src
        
        if src.startswith('~'):
            return path.expanduser(src)

        return path.join(self._cwd, src)

    def exists(self, src: str) -> bool:
        """Check whether a file or directory exists.

        Arguments:
            src (str): file or directory path

        Returns:
            bool: whether file or directory exists
        """
        return path.exists(self.abspath(src))

    def write(self, dst: str, txt: str, mode: str = 'w', sudo: bool = False):
        """Write to a text file and wait until write is complete.

        Arguments:
            dst (str): path of file to be written to
            txt (str): content of the file
            mode (str): mode to open the file to be written to (default: 'w')
            sudo (bool): allow writing to files not in ./scratch or ./output (default: False)

        """
        if not sudo:
            self._check(dst)

        with open(self.abspath(dst), mode) as f:
            f.write(txt)
            f.flush()
            fsync(f.fileno())

    def mkdir(self, dst: str, sudo: bool = False):
        """Create directory (ignore existing directories).

        Arguments:
            dst (str): directory path
            sudo (bool): allow create a directory that is not in ./scratch or ./output (default: False)
        """
        if not sudo:
            self._check(dst)

        check_call('mkdir -p ' + self.abspath(dst), shell=True)

    def rm(self, dst: str, sudo: bool = False):
        """Remove a file or directory.

        Args:
            dst (str): file or directory to be removed
            sudo (bool): allow removing locations not in ./scratch or ./output (default: False)
        """
        if not sudo:
            self._check(dst)

        check_call('rm -rf ' + self.abspath(dst), shell=True)

    def cp(self, src: str, dst: str, sudo: bool = False):
        """Copy a file or directory.

        Arguments:
            src (str): file or directory to be copied
            dst (str): destination
            sudo (bool): allow copying to locations not in ./scratch or ./output (default: False)
        """
        if not sudo:
            self._check(dst)

        check_call(f'cp -r {self.abspath(src)} {self.abspath(dst)}', shell=True)

    def mv(self, src: str, dst: str, sudo: bool = False):
        """Move a file or directory.

        Arguments:
            src (str): file or directory to be moved
            dst (str): destination
            sudo (bool): allow removing locations not in ./scratch or ./output (default: False)
        """
        if not sudo:
            self._check(src)
            self._check(dst)

        check_call(f'mv {self.abspath(src)} {self.abspath(dst)}', shell=True)

    def ln(self, src: str, dst: str, sudo: bool = False):
        """Create symbolic link.

        Arguments:
            src (str): file or directory to be linked
            dst (str): destination
            sudo (bool): allow linking to locations not in ./scratch or ./output (default: False)
        """
        if not sudo:
            self._check(dst)

        check_call(f'ln -s {self.abspath(src)} {self.abspath(dst)}', shell=True)

    def ls(self, src: str, reg: str = '*') -> List[str]:
        """Get items under target directory

        Arguments:
            src (str): directory path
            reg (str): regexp entry filter (default: no filter)

        Returns:
            List[str]: items under target directory
        """
        entries: List[str] = []

        for entry in glob(path.join(self.abspath(src), reg)):
            entries.append(entry.split('/')[-1])

        return entries

    def _check(self, src: str):
        """Check if pyper has write permission to target directory.

        To avoid unintended write to non-project directory, write operations can only be performed
        in ./scratch or ./output directory unless explicitly requested.

        Args:
            src (str): directory to be checked

        Raises:
            PermissionError: raise error when directory is not in ./scratch or ./output
        """
        src_abs = self.abspath(src)

        for dirname in self._dirs:
            if src_abs.startswith(self.abspath(dirname)):
                return

        raise PermissionError(f'no permission to write to {src}')


# public shell object
shell = Shell()
