"""
Table formatting utilities for TUI display.

This module provides standardized table formatting functions using Rich library
to ensure consistent display of tabular data throughout the application.
"""

from typing import Any, Dict, List, Union

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich import box

from ..ui.theme import TABLE_TITLE_STYLE, TABLE_BORDER_STYLE, HEADER_STYLE


def create_table(
    title: str = None,
    headers: List[str] = None,
    show_header: bool = True,
    show_lines: bool = True,
    box_style: str = "ROUNDED",
    padding: Union[int, tuple] = (0, 1),
    title_style: str = TABLE_TITLE_STYLE,
    header_style: str = HEADER_STYLE,
    border_style: str = TABLE_BORDER_STYLE,
    expand: bool = False,
) -> Table:
    """
    Create a Rich Table with consistent styling.

    Args:
        title: Optional title for the table
        headers: List of column headers
        show_header: Whether to display headers
        show_lines: Whether to show lines between rows
        box_style: Box style for the table (e.g., "ROUNDED", "MINIMAL", etc.)
        padding: Cell padding as (vertical, horizontal) or single value
        title_style: Style for the table title
        header_style: Style for column headers
        border_style: Style for table borders
        expand: Whether the table should expand to fill available width.
            Defaults to False so the table width matches its content.

    Returns:
        A configured Rich Table object
    """
    try:
        # Convert box_style to uppercase and get the corresponding box style
        box_style = box_style.upper()
        box_instance = getattr(box, box_style, box.ROUNDED)

        table = Table(
            title=title,
            show_header=show_header,
            show_lines=show_lines,
            box=box_instance,
            padding=padding,
            title_style=title_style,
            border_style=border_style,
            expand=expand,
        )

        # Add headers if provided
        if headers and show_header:
            for header in headers:
                table.add_column(header, style=header_style)

        return table
    except Exception as e:
        # Log error and fall back to default styling
        import logging

        logging.error(f"Error creating table: {e}")

        # Create a table with default styling as fallback
        table = Table(
            title=title,
            show_header=show_header,
            show_lines=show_lines,
            box=box.ROUNDED,
            padding=padding,
            title_style=title_style,
            border_style=border_style,
            expand=expand,
        )

        # Add headers if provided
        if headers and show_header:
            for header in headers:
                table.add_column(header, style=header_style)

        return table


def format_cell(value: Any, style: Any = None, none_display: str = "N/A") -> Text:
    """
    Format a cell value with consistent styling.

    Args:
        value: The value to format
        style: Optional Rich style to apply (string or function)
        none_display: What to display for None values

    Returns:
        A Rich Text object with formatted content
    """
    # Handle None values
    if value is None:
        return Text(none_display, style="dim italic")

    # Convert to string if not already
    if not isinstance(value, str):
        value = str(value)

    # If style is a function, call it with the value
    applied_style = style
    if callable(style):
        try:
            applied_style = style(value)
        except Exception as e:
            import logging

            logging.error(f"Error applying style function: {e}")
            applied_style = None

    return Text(value, style=applied_style)


def add_row_with_styles(
    table: Table,
    row_data: List[Any],
    styles: List[str] = None,
) -> None:
    """
    Add a row to a table with optional styling per cell.

    Args:
        table: The Rich Table to add the row to
        row_data: List of cell values
        styles: Optional list of styles to apply to each cell
    """
    # Initialize defaults if not provided
    if styles is None:
        styles = [None] * len(row_data)

    # Format each cell and add to table
    formatted_cells = [
        format_cell(data, style=style) for data, style in zip(row_data, styles)
    ]

    table.add_row(*formatted_cells)


def add_rows_from_data(
    table: Table,
    data: List[Dict[str, Any]],
    columns: List[str],
    style_map: Dict[str, Any] = None,
) -> None:
    """
    Add multiple rows from a list of dictionaries.

    Args:
        table: The Rich Table to add rows to
        data: List of dictionaries containing row data
        columns: List of column keys to extract from each dictionary
        style_map: Optional dictionary mapping column names to styles or style functions
               Style functions can take the value, or both value and row
    """
    if style_map is None:
        style_map = {}

    for item in data:
        row_data = []
        row_styles = []

        for col in columns:
            # Get the value for this column
            value = item.get(col)
            row_data.append(value)

            # Get the style for this column
            style_func_or_value = style_map.get(col)

            # If the style is callable, it might expect the row as additional context
            if callable(style_func_or_value):
                try:
                    # First try calling with both value and row
                    import inspect

                    sig = inspect.signature(style_func_or_value)
                    if len(sig.parameters) > 1:
                        # Function accepts multiple parameters, pass value and row
                        style = style_func_or_value(value, item)
                    else:
                        # Function accepts only one parameter, just pass value
                        style = style_func_or_value(value)
                except Exception:
                    # Fall back to just passing the value
                    try:
                        style = style_func_or_value(value)
                    except Exception:
                        style = None
            else:
                # Not a function, just use the style value directly
                style = style_func_or_value

            row_styles.append(style)

        add_row_with_styles(table, row_data, row_styles)


def display_table(
    console: Console,
    data: List[Dict[str, Any]],
    columns: List[str],
    headers: List[str] = None,
    title: str = None,
    style_map: Dict[str, Any] = None,
    title_style: str = TABLE_TITLE_STYLE,
    header_style: str = HEADER_STYLE,
    border_style: str = TABLE_BORDER_STYLE,
    show_header: bool = True,
    show_lines: bool = True,
    box_style: str = "ROUNDED",
    padding: Union[int, tuple] = (0, 1),
    expand: bool = False,
) -> None:
    """
    Create, populate and display a table in one operation.

    This is the main function to use for displaying tabular data in the TUI.
    It handles various formatting options including styling, truncation,
    and customizable table appearance.

    Args:
        console: Rich Console to display the table on
        data: List of dictionaries containing row data
        columns: List of column keys to extract from each dictionary
        headers: Column headers (defaults to columns if not provided)
        title: Optional table title
        style_map: Optional dictionary mapping column names to styles or style functions
               Style functions should accept a value and return a style string
        title_style: Style for the table title
        header_style: Style for the column headers
        border_style: Style for table borders
        show_header: Whether to show the header row
        show_lines: Whether to show lines between rows
        box_style: Box style for the table (e.g., "ROUNDED")
        padding: Cell padding as int or tuple
        expand: Whether the table should expand to fill available width. Defaults
            to False so the table width matches its content.
    """
    try:
        # Use columns as headers if not provided
        if headers is None:
            headers = columns

        # Create the table
        table = create_table(
            title=title,
            headers=headers,
            title_style=title_style,
            header_style=header_style,
            border_style=border_style,
            show_header=show_header,
            show_lines=show_lines,
            box_style=box_style,
            padding=padding,
            expand=expand,
        )

        # Handle empty data case
        if not data:
            console.print(table)
            return

        # Add rows
        add_rows_from_data(table, data, columns, style_map)

        # Display the table
        console.print(table)

    except Exception as e:
        # Log the error and fall back to a simpler display method
        import logging

        logging.error(f"Error displaying table: {e}")

        # Print a simple error message and the raw data as fallback
        console.print(f"[red]Error displaying formatted table: {e}[/red]")
        console.print(f"Raw data: {data[:5]}" + ("..." if len(data) > 5 else ""))
