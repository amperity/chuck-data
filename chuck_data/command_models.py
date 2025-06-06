"""
Command models for Chuck application.
Defines structured request objects for commands.
"""

from typing import Dict, Any
from dataclasses import dataclass

# Import the CommandResult class instead of defining CommandResponse


@dataclass
class CommandRequest:
    """Request object for commands"""

    command: str
    params: Dict[str, Any]


# Note: CommandResponse has been replaced by CommandResult from chuck_data.commands.base
