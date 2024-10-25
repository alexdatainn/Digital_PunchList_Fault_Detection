import glob
import logging
import os
from importlib import import_module
from typing import Any, Callable, Dict, List


class PluginManager:
    def __init__(self) -> None:
        self.plugins: Dict[str, Callable] = {}

    def load_plugins(self, plugin_folder: str) -> None:
        file_list: List[str] = glob.glob(f"{plugin_folder}/*.py")

        for file in file_list:
            module: str = os.path.splitext(os.path.basename(file))[0]

            if module.startswith("_"):
                continue

            try:
                self.plugins[module] = getattr(
                    import_module(
                        f"{plugin_folder.replace('/', '.')}.{module}"
                    ),
                    "run",
                )
            except AttributeError:
                logging.warning(
                    f"{file} does not contain the function 'run', ignoring"
                )

    def run_module(self, module, *args) -> Any:
        if module not in self.plugins:
            logging.error(f"Plugin '{module}' does not exist")
            return

        return self.plugins[module](*args)

    def run_module_internally(module, *args) -> Any:
        plugin_module = getattr(
                    import_module(
                        f"{module}"
                    ),
                    "run",
                )
        return plugin_module(*args)
