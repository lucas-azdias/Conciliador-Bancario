import importlib
import pkgutil
import sys


# Import all submodules in the models package
package = sys.modules[__name__]

for loader, module_name, is_pkg in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{module_name}")