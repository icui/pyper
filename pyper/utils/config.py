import json
from importlib import import_module
from pkgutil import iter_modules
from typing import Callable, List, Union, Dict, Any

from pyper import shell


class Config:
    """Manage entries in config.json."""
    # entries from config.json
    _config: Dict[str, Dict[str, Any]]

    def __init__(self):
        """Read from config.json."""
        if shell.exists('config.json'):
            with open('config.json') as f:
                self._config = json.load(f)

        else:
            self._config = {}

    def get(self, section: str, key: str) -> Any:
        """Get configuration from config.json.

        Args:
            section (str): section in config.json
            key (str): key in the section

        Returns:
            Any: entry value
        """
        # return None if entry not in config.json
        if section not in self._config or key not in self._config[section]:
            return None

        return self._config[section][key]

    def get_input(self, section: str, key: str, prompt: str, cast: Union[List, Callable] = str) -> Any:
        """Get configuration from config.json, ask for user input if entry is missing.

        Args:
            section (str): section in config.json
            key (str): key in the section
            prompt (str): prompt message for user input
            cast (Union[List, Callable]): cast user input to non-str value (default: None)

        Returns:
            Any: entry value
        """
        # return value in config.json if exists
        val = self.get(section, key)
        if val is not None:
            return val

        # get path to the entry
        if section not in self._config:
            self._config[section] = {}

        # select from list
        input_prompt = prompt

        if isinstance(cast, list):
            input_prompt += '\n' + '\n'.join(f'{i + 1}) {cast[i]}' for i in range(len(cast)))

        # get value from user input
        val = input(input_prompt + '\n')

        if not val:
            # re-enter empty input
            return self.get_input(section, key, prompt, cast)

        if isinstance(cast, list):
            # return list entry
            try:
                index = int(val) - 1
                if index < 0:
                    raise ValueError

                val = cast[index]
            
            except Exception:
                return self.get_input(section, key, 'Please enter a valid number.', cast)

        elif callable(cast):
            # return converted type
            try:
                val = cast(val)
            
            except Exception:
                return self.get_input(section, key, f'Please enter a valid {cast} value.', cast)

        self._config[section][key] = val

        # write user input to config.json
        with open('config.json', 'w') as f:
            json.dump(self._config, f, indent=4)

        return val

    def get_module(self, path: str) -> Any:
        """Import a module based on config.json or user input.

        Args:
            path (str): import path of the module

        Returns:
            Any: imported module
        """
        # get module name from config.json
        name = path.split('.')[-1]
        module = self.get('module', name)

        # get module name from input
        if module is None:
            # get module list
            namespace: Any = import_module(path)
            modnames = list(info[1] for info in iter_modules(namespace.__path__) if info[1] != 'base')

            # get input
            module = self.get_input('module', name, f'Select module in {path}:', modnames)

        # load module
        return import_module(f'{path}.{module}')


# public config object
config = Config()
