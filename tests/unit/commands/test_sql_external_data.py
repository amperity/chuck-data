"""
Tests for sql_external_data utility module.

These tests verify the functionality of external SQL data fetching and pagination
utilities used when SQL queries return large result sets with external links.
"""

import pytest
from unittest.mock import patch, Mock
import requests
import csv
import io

from chuck_data.commands.sql_external_data import (
    fetch_external_data,
    fetch_chunk_data,
    get_paginated_rows,
    PaginatedSQLResult,
)


class TestFetchExternalData:
    """Test fetch_external_data function."""

    @patch("chuck_data.commands.sql_external_data.requests.get")
    def test_fetch_external_data_success(self, mock_get):
        """fetch_external_data successfully fetches and parses CSV data."""
        # Setup mock response with CSV data
        csv_data = "name,age,city\nJohn,30,NYC\nJane,25,LA\nBob,35,Chicago"
        mock_response = Mock()
        mock_response.text = csv_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_external_data("https://databricks.com/results/chunk1.csv")

        # Verify the request was made correctly
        mock_get.assert_called_once_with(
            "https://databricks.com/results/chunk1.csv", timeout=30
        )

        # Verify the parsed data
        expected_rows = [
            ["name", "age", "city"],
            ["John", "30", "NYC"],
            ["Jane", "25", "LA"],
            ["Bob", "35", "Chicago"],
        ]
        assert result == expected_rows

    @patch("chuck_data.commands.sql_external_data.requests.get")
    def test_fetch_external_data_with_custom_timeout(self, mock_get):
        """fetch_external_data respects custom timeout parameter."""
        mock_response = Mock()
        mock_response.text = "col1\nvalue1"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetch_external_data("https://test.com/data.csv", timeout=60)

        mock_get.assert_called_once_with("https://test.com/data.csv", timeout=60)

    def test_fetch_external_data_invalid_url(self):
        """fetch_external_data raises ValueError for invalid URLs."""
        with pytest.raises(ValueError, match="Invalid URL"):
            fetch_external_data("not-a-valid-url")

        with pytest.raises(ValueError, match="Invalid URL"):
            fetch_external_data("missing-protocol.com")

    @patch("chuck_data.commands.sql_external_data.requests.get")
    def test_fetch_external_data_http_error(self, mock_get):
        """fetch_external_data raises RequestException for HTTP errors."""
        mock_get.side_effect = requests.RequestException("Connection timeout")

        with pytest.raises(requests.RequestException, match="Connection timeout"):
            fetch_external_data("https://databricks.com/results/chunk1.csv")

    @patch("chuck_data.commands.sql_external_data.requests.get")
    def test_fetch_external_data_http_status_error(self, mock_get):
        """fetch_external_data raises HTTPError for bad status codes."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError, match="404 Not Found"):
            fetch_external_data("https://databricks.com/results/missing.csv")

    @patch("chuck_data.commands.sql_external_data.requests.get")
    def test_fetch_external_data_csv_parsing_error(self, mock_get):
        """fetch_external_data handles CSV parsing errors."""
        # Setup mock response with malformed CSV
        mock_response = Mock()
        mock_response.text = 'name,age\n"unclosed quote field,30\n'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Mock csv.reader to raise an error
        with patch("csv.reader") as mock_csv_reader:
            mock_csv_reader.side_effect = csv.Error("Error parsing CSV")

            with pytest.raises(csv.Error, match="Error parsing CSV"):
                fetch_external_data("https://databricks.com/results/bad.csv")

    @patch("chuck_data.commands.sql_external_data.requests.get")
    def test_fetch_external_data_empty_csv(self, mock_get):
        """fetch_external_data handles empty CSV files."""
        mock_response = Mock()
        mock_response.text = ""
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_external_data("https://databricks.com/results/empty.csv")

        assert result == []

    @patch("chuck_data.commands.sql_external_data.requests.get")
    def test_fetch_external_data_single_row(self, mock_get):
        """fetch_external_data handles single row CSV files."""
        mock_response = Mock()
        mock_response.text = "single_value"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_external_data("https://databricks.com/results/single.csv")

        assert result == [["single_value"]]


class TestFetchChunkData:
    """Test fetch_chunk_data function."""

    @patch("chuck_data.commands.sql_external_data.fetch_external_data")
    def test_fetch_chunk_data_success(self, mock_fetch):
        """fetch_chunk_data successfully fetches data for specified chunk."""
        # Setup mock external data
        mock_fetch.return_value = [["id", "name"], ["1", "John"], ["2", "Jane"]]

        external_links = [
            {
                "chunk_index": 0,
                "external_link": "https://databricks.com/chunk0.csv",
                "row_count": 2,
            },
            {
                "chunk_index": 1,
                "external_link": "https://databricks.com/chunk1.csv",
                "row_count": 3,
            },
        ]

        result = fetch_chunk_data(external_links, 0)

        # Verify correct external link was fetched
        mock_fetch.assert_called_once_with("https://databricks.com/chunk0.csv")
        assert result == [["id", "name"], ["1", "John"], ["2", "Jane"]]

    def test_fetch_chunk_data_chunk_not_found(self):
        """fetch_chunk_data returns None when chunk_index not found."""
        external_links = [
            {"chunk_index": 0, "external_link": "https://databricks.com/chunk0.csv"}
        ]

        result = fetch_chunk_data(external_links, 5)  # Non-existent chunk_index

        assert result is None

    def test_fetch_chunk_data_missing_external_link(self):
        """fetch_chunk_data returns None when external_link is missing."""
        external_links = [{"chunk_index": 0}]  # Missing external_link

        result = fetch_chunk_data(external_links, 0)

        assert result is None

    def test_fetch_chunk_data_empty_external_links(self):
        """fetch_chunk_data returns None for empty external_links list."""
        result = fetch_chunk_data([], 0)

        assert result is None

    @patch("chuck_data.commands.sql_external_data.fetch_external_data")
    def test_fetch_chunk_data_fetch_error_bubbles_up(self, mock_fetch):
        """fetch_chunk_data bubbles up exceptions from fetch_external_data."""
        mock_fetch.side_effect = requests.RequestException("Network error")

        external_links = [
            {"chunk_index": 0, "external_link": "https://databricks.com/chunk0.csv"}
        ]

        with pytest.raises(requests.RequestException, match="Network error"):
            fetch_chunk_data(external_links, 0)


class TestGetPaginatedRows:
    """Test get_paginated_rows function."""

    @patch("chuck_data.commands.sql_external_data.fetch_chunk_data")
    def test_get_paginated_rows_single_chunk(self, mock_fetch_chunk):
        """get_paginated_rows fetches rows from single chunk."""
        # Setup mock chunk data
        mock_fetch_chunk.return_value = [
            ["id", "name"],
            ["1", "Alice"],
            ["2", "Bob"],
            ["3", "Carol"],
        ]

        external_links = [
            {
                "chunk_index": 0,
                "external_link": "https://databricks.com/chunk0.csv",
                "row_count": 4,
            }
        ]

        # Request rows 1-2 (skip header)
        result = get_paginated_rows(external_links, start_row=1, num_rows=2)

        mock_fetch_chunk.assert_called_once()
        assert result == [["1", "Alice"], ["2", "Bob"]]

    @patch("chuck_data.commands.sql_external_data.fetch_chunk_data")
    def test_get_paginated_rows_multiple_chunks(self, mock_fetch_chunk):
        """get_paginated_rows fetches rows spanning multiple chunks."""

        # Setup mock responses for different chunks
        def mock_fetch_side_effect(links, chunk_index):
            if chunk_index == 0:
                return [["1", "Alice"], ["2", "Bob"]]
            elif chunk_index == 1:
                return [["3", "Carol"], ["4", "Dave"], ["5", "Eve"]]
            return []

        mock_fetch_chunk.side_effect = mock_fetch_side_effect

        external_links = [
            {"chunk_index": 0, "row_count": 2},
            {"chunk_index": 1, "row_count": 3},
        ]

        # Request 3 rows starting from row 1 (should span both chunks)
        result = get_paginated_rows(external_links, start_row=1, num_rows=3)

        assert mock_fetch_chunk.call_count == 2
        assert result == [["2", "Bob"], ["3", "Carol"], ["4", "Dave"]]

    @patch("chuck_data.commands.sql_external_data.fetch_chunk_data")
    def test_get_paginated_rows_chunk_fetch_error(self, mock_fetch_chunk):
        """get_paginated_rows continues despite chunk fetch errors."""

        # First chunk fails, second succeeds
        def mock_fetch_side_effect(links, chunk_index):
            if chunk_index == 0:
                raise requests.RequestException("Chunk 0 failed")
            elif chunk_index == 1:
                return [["3", "Carol"], ["4", "Dave"]]
            return []

        mock_fetch_chunk.side_effect = mock_fetch_side_effect

        external_links = [
            {"chunk_index": 0, "row_count": 2},
            {"chunk_index": 1, "row_count": 2},
        ]

        # Should continue and return data from successful chunk
        result = get_paginated_rows(external_links, start_row=2, num_rows=2)

        assert result == [["3", "Carol"], ["4", "Dave"]]

    def test_get_paginated_rows_empty_external_links(self):
        """get_paginated_rows handles empty external_links."""
        result = get_paginated_rows([], start_row=0, num_rows=10)

        assert result == []

    @patch("chuck_data.commands.sql_external_data.fetch_chunk_data")
    def test_get_paginated_rows_unsorted_chunks(self, mock_fetch_chunk):
        """get_paginated_rows sorts chunks by chunk_index."""

        # Setup chunks in reverse order
        def mock_fetch_side_effect(links, chunk_index):
            if chunk_index == 0:
                return [["1", "Alice"], ["2", "Bob"]]
            elif chunk_index == 1:
                return [["3", "Carol"], ["4", "Dave"]]
            return []

        mock_fetch_chunk.side_effect = mock_fetch_side_effect

        # External links provided in reverse order
        external_links = [
            {"chunk_index": 1, "row_count": 2},  # Should be processed second
            {"chunk_index": 0, "row_count": 2},  # Should be processed first
        ]

        result = get_paginated_rows(external_links, start_row=0, num_rows=4)

        # Should return rows in correct order (chunks sorted by index)
        assert result == [["1", "Alice"], ["2", "Bob"], ["3", "Carol"], ["4", "Dave"]]

    @patch("chuck_data.commands.sql_external_data.fetch_chunk_data")
    def test_get_paginated_rows_exact_page_boundary(self, mock_fetch_chunk):
        """get_paginated_rows handles requests at exact chunk boundaries."""
        mock_fetch_chunk.return_value = [["3", "Carol"], ["4", "Dave"]]

        external_links = [
            {"chunk_index": 0, "row_count": 2},
            {"chunk_index": 1, "row_count": 2},
        ]

        # Request exactly at chunk boundary
        result = get_paginated_rows(external_links, start_row=2, num_rows=2)

        # Should fetch second chunk only
        mock_fetch_chunk.assert_called_once()
        assert result == [["3", "Carol"], ["4", "Dave"]]


class TestPaginatedSQLResult:
    """Test PaginatedSQLResult class."""

    def test_paginated_sql_result_initialization(self):
        """PaginatedSQLResult initializes correctly."""
        columns = ["id", "name", "age"]
        external_links = [{"chunk_index": 0, "row_count": 50}]
        total_row_count = 150
        chunks = [{"row_offset": 0, "row_count": 50}]

        result = PaginatedSQLResult(columns, external_links, total_row_count, chunks)

        assert result.columns == columns
        assert result.external_links == external_links
        assert result.total_row_count == total_row_count
        assert result.chunks == chunks
        assert result.current_position == 0
        assert result.page_size == 50

    @patch("chuck_data.commands.sql_external_data.get_paginated_rows")
    def test_get_next_page_success(self, mock_get_paginated):
        """get_next_page returns correct data and advances position."""
        mock_get_paginated.return_value = [
            ["1", "Alice", "25"],
            ["2", "Bob", "30"],
            ["3", "Carol", "28"],
        ]

        result = PaginatedSQLResult(
            columns=["id", "name", "age"],
            external_links=[{"chunk_index": 0}],
            total_row_count=100,
            chunks=[],
        )

        rows, has_more = result.get_next_page()

        # Verify the call to get_paginated_rows
        mock_get_paginated.assert_called_once_with([{"chunk_index": 0}], 0, 50)

        # Verify returned data
        assert rows == [["1", "Alice", "25"], ["2", "Bob", "30"], ["3", "Carol", "28"]]
        assert has_more is True  # More pages available (position 3 < total 100)
        assert result.current_position == 3

    @patch("chuck_data.commands.sql_external_data.get_paginated_rows")
    def test_get_next_page_last_page(self, mock_get_paginated):
        """get_next_page correctly identifies last page."""
        mock_get_paginated.return_value = [["98", "Second Last"], ["99", "Last"]]

        result = PaginatedSQLResult(
            columns=["id", "name"],
            external_links=[{"chunk_index": 0}],
            total_row_count=100,
            chunks=[],
        )
        result.current_position = 98  # Near end

        rows, has_more = result.get_next_page()

        assert rows == [["98", "Second Last"], ["99", "Last"]]
        assert has_more is False  # No more pages (position 100 >= total 100)
        assert result.current_position == 100

    @patch("chuck_data.commands.sql_external_data.get_paginated_rows")
    def test_get_next_page_beyond_end(self, mock_get_paginated):
        """get_next_page returns empty when beyond end."""
        result = PaginatedSQLResult(
            columns=["id", "name"],
            external_links=[{"chunk_index": 0}],
            total_row_count=100,
            chunks=[],
        )
        result.current_position = 100  # At end

        rows, has_more = result.get_next_page()

        # Should not call get_paginated_rows
        mock_get_paginated.assert_not_called()
        assert rows == []
        assert has_more is False
        assert result.current_position == 100  # Position unchanged

    @patch("chuck_data.commands.sql_external_data.get_paginated_rows")
    def test_get_next_page_partial_page(self, mock_get_paginated):
        """get_next_page handles partial pages correctly."""
        # Return fewer rows than page_size
        mock_get_paginated.return_value = [["48", "Near End"], ["49", "Last"]]

        result = PaginatedSQLResult(
            columns=["id", "name"],
            external_links=[{"chunk_index": 0}],
            total_row_count=50,
            chunks=[],
        )
        result.current_position = 48

        rows, has_more = result.get_next_page()

        assert rows == [["48", "Near End"], ["49", "Last"]]
        assert has_more is False  # No more pages (position 50 >= total 50)
        assert result.current_position == 50

    def test_reset_pagination(self):
        """reset method resets pagination to beginning."""
        result = PaginatedSQLResult(
            columns=["id"],
            external_links=[],
            total_row_count=100,
            chunks=[],
        )
        result.current_position = 75  # Advance position

        result.reset()

        assert result.current_position == 0

    @patch("chuck_data.commands.sql_external_data.get_paginated_rows")
    def test_multiple_pages_pagination(self, mock_get_paginated):
        """Multiple get_next_page calls work correctly."""
        # Setup different responses for different pages
        page_responses = [
            [["1", "Alice"], ["2", "Bob"]],  # Page 1
            [["3", "Carol"], ["4", "Dave"]],  # Page 2
            [["5", "Eve"]],  # Page 3 (partial)
        ]
        mock_get_paginated.side_effect = page_responses

        result = PaginatedSQLResult(
            columns=["id", "name"],
            external_links=[{"chunk_index": 0}],
            total_row_count=5,
            chunks=[],
        )
        result.page_size = 2  # Small page size for testing

        # Get first page
        rows1, has_more1 = result.get_next_page()
        assert rows1 == [["1", "Alice"], ["2", "Bob"]]
        assert has_more1 is True
        assert result.current_position == 2

        # Get second page
        rows2, has_more2 = result.get_next_page()
        assert rows2 == [["3", "Carol"], ["4", "Dave"]]
        assert has_more2 is True
        assert result.current_position == 4

        # Get third (last) page
        rows3, has_more3 = result.get_next_page()
        assert rows3 == [["5", "Eve"]]
        assert has_more3 is False
        assert result.current_position == 5

        # Verify calls to get_paginated_rows
        expected_calls = [
            ([{"chunk_index": 0}], 0, 2),
            ([{"chunk_index": 0}], 2, 2),
            ([{"chunk_index": 0}], 4, 2),
        ]
        actual_calls = [call.args for call in mock_get_paginated.call_args_list]
        assert actual_calls == expected_calls

    @patch("chuck_data.commands.sql_external_data.get_paginated_rows")
    def test_get_next_page_with_error_handling(self, mock_get_paginated):
        """get_next_page handles errors from get_paginated_rows."""
        mock_get_paginated.side_effect = requests.RequestException("Network error")

        result = PaginatedSQLResult(
            columns=["id", "name"],
            external_links=[{"chunk_index": 0}],
            total_row_count=100,
            chunks=[],
        )

        # Should propagate the exception
        with pytest.raises(requests.RequestException, match="Network error"):
            result.get_next_page()

        # Position should remain unchanged
        assert result.current_position == 0
