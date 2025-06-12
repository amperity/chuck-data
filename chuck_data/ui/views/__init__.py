"""
View modules for displaying command results.

Each view module contains a View class that implements the BaseView interface
and registers itself with the view_registry.
"""

# Import all view modules to ensure they register with the view registry
from . import catalogs  # noqa: F401
from . import schemas  # noqa: F401
from . import tables  # noqa: F401
from . import models  # noqa: F401
from . import warehouses  # noqa: F401
from . import volumes  # noqa: F401
from . import status  # noqa: F401
