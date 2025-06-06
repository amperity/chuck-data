"""
Custom exceptions for the clamp application.
"""


class PaginationCancelled(Exception):
    """
    Exception raised when user cancels pagination.

    This is used to signal that pagination was cancelled by user action
    (pressing 'q') and should bubble up to return control to the main prompt.
    """

    pass
