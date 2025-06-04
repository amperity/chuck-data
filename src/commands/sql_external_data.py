"""
Utility for fetching and parsing external SQL result data from Databricks.

When SQL queries return large result sets, Databricks provides external_links
to CSV files containing the data. This module handles fetching and parsing
that external data.
"""

import csv
import io
import logging
import requests
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse


def fetch_external_data(external_link: str, timeout: int = 30) -> List[List[str]]:
    """
    Fetch CSV data from an external link and parse it into rows.

    Args:
        external_link: URL to fetch CSV data from
        timeout: Request timeout in seconds

    Returns:
        List of rows, where each row is a list of string values

    Raises:
        requests.RequestException: If HTTP request fails
        csv.Error: If CSV parsing fails
    """
    try:
        logging.debug(f"Fetching external SQL data from: {external_link}")

        # Validate URL
        parsed_url = urlparse(external_link)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid URL: {external_link}")

        # Fetch the CSV data
        response = requests.get(external_link, timeout=timeout)
        response.raise_for_status()

        # Parse CSV data
        csv_content = response.text
        csv_reader = csv.reader(io.StringIO(csv_content))
        rows = list(csv_reader)

        logging.debug(f"Successfully fetched {len(rows)} rows from external link")
        return rows

    except requests.RequestException as e:
        logging.error(f"Failed to fetch external data from {external_link}: {e}")
        raise
    except csv.Error as e:
        logging.error(f"Failed to parse CSV data from {external_link}: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error fetching external data: {e}")
        raise


def fetch_chunk_data(
    external_links: List[Dict[str, Any]], chunk_index: int
) -> Optional[List[List[str]]]:
    """
    Fetch data for a specific chunk by chunk_index.

    Args:
        external_links: List of external link objects from Databricks API response
        chunk_index: Index of the chunk to fetch

    Returns:
        List of rows for the specified chunk, or None if chunk not found
    """
    # Find the external link for the specified chunk
    target_link = None
    for link in external_links:
        if link.get("chunk_index") == chunk_index:
            target_link = link
            break

    if not target_link:
        logging.warning(f"No external link found for chunk_index {chunk_index}")
        return None

    external_url = target_link.get("external_link")
    if not external_url:
        logging.warning(f"No external_link URL found in chunk {chunk_index}")
        return None

    try:
        return fetch_external_data(external_url)
    except Exception as e:
        logging.error(f"Failed to fetch chunk {chunk_index}: {e}")
        raise


def get_paginated_rows(
    external_links: List[Dict[str, Any]], start_row: int, num_rows: int = 50
) -> List[List[str]]:
    """
    Get a specific page of rows from external links.

    Args:
        external_links: List of external link objects from Databricks API response
        start_row: Starting row index (0-based)
        num_rows: Number of rows to fetch

    Returns:
        List of rows for the requested page
    """
    # Sort external links by chunk_index to ensure proper order
    sorted_links = sorted(external_links, key=lambda x: x.get("chunk_index", 0))

    current_row = 0
    result_rows = []

    for link in sorted_links:
        chunk_row_count = link.get("row_count", 0)
        chunk_start = current_row
        chunk_end = current_row + chunk_row_count

        # Check if this chunk contains any of our target rows
        if start_row < chunk_end and current_row < start_row + num_rows:
            # We need some data from this chunk
            try:
                chunk_data = fetch_chunk_data([link], link.get("chunk_index"))
                if chunk_data:
                    # Calculate which rows from this chunk we need
                    local_start = max(0, start_row - chunk_start)
                    local_end = min(chunk_row_count, start_row + num_rows - chunk_start)

                    if local_start < len(chunk_data):
                        chunk_slice = chunk_data[local_start:local_end]
                        result_rows.extend(chunk_slice)

                        # If we have enough rows, we're done
                        if len(result_rows) >= num_rows:
                            return result_rows[:num_rows]
            except Exception as e:
                logging.error(f"Failed to fetch chunk {link.get('chunk_index')}: {e}")
                # Continue with other chunks

        current_row += chunk_row_count

        # If we've passed our target range, we're done
        if current_row >= start_row + num_rows:
            break

    return result_rows


class PaginatedSQLResult:
    """
    Class to manage paginated SQL results with external data fetching.
    """

    def __init__(
        self,
        columns: List[str],
        external_links: List[Dict[str, Any]],
        total_row_count: int,
        chunks: List[Dict[str, Any]],
    ):
        self.columns = columns
        self.external_links = external_links
        self.total_row_count = total_row_count
        self.chunks = chunks
        self.current_position = 0
        self.page_size = 50

    def get_next_page(self) -> tuple[List[List[str]], bool]:
        """
        Get the next page of results.

        Returns:
            Tuple of (rows, has_more) where rows is list of data rows
            and has_more indicates if there are more pages available
        """
        if self.current_position >= self.total_row_count:
            return [], False

        rows = get_paginated_rows(
            self.external_links, self.current_position, self.page_size
        )

        self.current_position += len(rows)
        has_more = self.current_position < self.total_row_count

        return rows, has_more

    def reset(self):
        """Reset pagination to the beginning."""
        self.current_position = 0
