from importlib import import_module  # re-export helpers for tests

# convenience imports in ui namespace
from . import format_utils  # noqa: F401
from . import styles  # noqa: F401

# Import views to register them with the view_registry
import chuck_data.ui.views.catalogs  # noqa: F401
import chuck_data.ui.views.schemas  # noqa: F401
import chuck_data.ui.views.tables  # noqa: F401
import chuck_data.ui.views.models  # noqa: F401
import chuck_data.ui.views.warehouses  # noqa: F401
import chuck_data.ui.views.volumes  # noqa: F401
import chuck_data.ui.views.status  # noqa: F401
