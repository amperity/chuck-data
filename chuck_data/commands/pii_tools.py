"""
PII handling utilities for command handlers.

This module contains helper functions for detecting and tagging PII (Personally
Identifiable Information) in database tables.
"""

import logging
import json
import concurrent.futures
from typing import Dict, Any, Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.llm.client import LLMClient


def _helper_tag_pii_columns_logic(
    databricks_client: DatabricksAPIClient,
    llm_client_instance: LLMClient,
    table_name_param: str,
    catalog_name_context: Optional[str] = None,
    schema_name_context: Optional[str] = None,
) -> Dict[str, Any]:
    """Internal logic for PII tagging of a single table."""
    response_content_for_error = ""
    try:
        # Resolve full table name using APIs directly instead of handler
        table_details_kwargs = {"full_name": table_name_param}
        if catalog_name_context and schema_name_context and "." not in table_name_param:
            # Only a table name was provided, construct full name
            full_name = (
                f"{catalog_name_context}.{schema_name_context}.{table_name_param}"
            )
            table_details_kwargs = {"full_name": full_name}

        try:
            # Use direct API call instead of handle_table
            table_info = databricks_client.get_table(**table_details_kwargs)
            if not table_info:
                error_msg = f"Failed to retrieve table details for PII tagging: {table_name_param}"
                return {
                    "error": error_msg,
                    "table_name_param": table_name_param,
                    "skipped": True,
                }

            resolved_full_name = table_info.get("full_name", table_name_param)
            columns = table_info.get("columns", [])
        except Exception as e:
            error_msg = f"Failed to retrieve table details: {str(e)}"
            return {
                "error": error_msg,
                "table_name_param": table_name_param,
                "skipped": True,
            }  # Skipped due to error

        base_name_of_resolved = resolved_full_name.split(".")[-1]

        if base_name_of_resolved.startswith("_stitch"):
            return {
                "skipped": True,
                "reason": f"Table '{resolved_full_name}' starts with _stitch.",
                "full_name": resolved_full_name,
                "table_name": base_name_of_resolved,
            }

        if not columns:
            return {
                "table_name": base_name_of_resolved,
                "full_name": resolved_full_name,
                "column_count": 0,
                "pii_column_count": 0,
                "has_pii": False,
                "columns": [],
                "pii_columns": [],
                "skipped": False,
            }

        # Use the LLM client instance passed to the function
        column_details_for_llm = [
            {"name": col.get("name", ""), "type": col.get("type_name", "")}
            for col in columns
        ]

        system_message = (
            "You are an expert PII detection assistant. Your task is to analyze a list of database columns (name and type) "
            "and assign a PII semantic tag to each column if applicable. Use ONLY the following PII semantic tags: "
            "address, address2, birthdate, city, country, create-dt, email, full-name, gender, generational-suffix, "
            "given-name, phone, postal, state, surname, title, update-dt. If a column does not contain PII, assign null. "
            "Respond ONLY with a valid JSON list of objects, where each object represents a column and has the following structure: "
            '{"name": "column_name", "semantic": "pii_tag_or_null"}. '
            "Maintain original order. No explanations or introductory text."
        )
        user_prompt = f"Analyze the following columns from table '{resolved_full_name}' and provide PII semantic tags in the specified JSON format: {json.dumps(column_details_for_llm, indent=2)}"

        llm_response_obj = llm_client_instance.chat(
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_prompt},
            ]
        )
        response_content_for_error = llm_response_obj.choices[
            0
        ].message.content  # Store for potential error reporting
        response_content_clean = response_content_for_error.strip()
        if response_content_clean.startswith("```json"):
            response_content_clean = response_content_clean[7:-3].strip()
        elif response_content_clean.startswith("```"):
            response_content_clean = response_content_clean[3:-3].strip()

        llm_tags = json.loads(response_content_clean)
        if not isinstance(llm_tags, list) or len(llm_tags) != len(columns):
            raise ValueError(
                f"LLM PII tag response format error. Expected {len(columns)} items, got {len(llm_tags)}."
            )

        semantic_map = {
            item["name"]: item["semantic"]
            for item in llm_tags
            if isinstance(item, dict) and "name" in item
        }
        tagged_columns_list = []
        for col in columns:
            col_name = col.get("name", "")
            tagged_columns_list.append(
                {
                    "name": col_name,
                    "type": col.get("type_name", ""),
                    "semantic": semantic_map.get(col_name),
                }
            )

        pii_cols = [col for col in tagged_columns_list if col["semantic"]]
        return {
            "table_name": base_name_of_resolved,
            "full_name": resolved_full_name,
            "column_count": len(columns),
            "pii_column_count": len(pii_cols),
            "has_pii": bool(pii_cols),
            "columns": tagged_columns_list,
            "pii_columns": pii_cols,
            "skipped": False,
        }
    except json.JSONDecodeError as e_json:
        logging.error(
            f"_helper_tag_pii_columns_logic: JSONDecodeError: {e_json} from LLM response: {response_content_for_error[:500]}"
        )  # Log more of the response
        return {"error": f"Failed to parse PII LLM response: {e_json}", "skipped": True}
    except Exception as e_tag:
        logging.error(
            f"_helper_tag_pii_columns_logic error for '{table_name_param}': {e_tag}",
            exc_info=True,
        )
        return {
            "error": f"Error during PII tagging for '{table_name_param}': {str(e_tag)}",
            "skipped": True,
        }


def _helper_scan_schema_for_pii_logic(
    client: DatabricksAPIClient,
    llm_client_instance: LLMClient,
    catalog_name: str,
    schema_name: str,
) -> Dict[str, Any]:
    """Internal logic for scanning all tables in a schema for PII."""
    if not catalog_name or not schema_name:
        return {"error": "Catalog and schema names are required for bulk PII scan."}

    # Use direct API call instead of handle_tables
    try:
        tables_response = client.list_tables(
            catalog_name=catalog_name, schema_name=schema_name, omit_columns=True
        )
        all_tables_in_schema = tables_response.get("tables", [])
    except Exception as e:
        return {
            "error": f"Failed to list tables for {catalog_name}.{schema_name}: {str(e)}"
        }

    tables_to_scan_summaries = [
        tbl
        for tbl in all_tables_in_schema
        if isinstance(tbl, dict) and not tbl.get("name", "").startswith("_stitch")
    ]

    if not tables_to_scan_summaries:
        return {
            "message": f"No user tables (excluding _stitch*) found in {catalog_name}.{schema_name}.",
            "catalog": catalog_name,
            "schema": schema_name,
            "tables_scanned": 0,
            "tables_with_pii": 0,
            "total_pii_columns": 0,
            "results_detail": [],
        }

    logging.info(
        f"Starting PII Scan for {len(tables_to_scan_summaries)} tables in {catalog_name}.{schema_name}."
    )
    scan_results_detail = []
    MAX_WORKERS = 5
    futures_map = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for table_summary_dict in tables_to_scan_summaries:
            table_name_only = table_summary_dict.get("name")
            if not table_name_only:
                continue
            # Pass client and context to the helper
            futures_map[
                executor.submit(
                    _helper_tag_pii_columns_logic,
                    client,
                    llm_client_instance,
                    table_name_only,
                    catalog_name,
                    schema_name,
                )
            ] = f"{catalog_name}.{schema_name}.{table_name_only}"

        for future in concurrent.futures.as_completed(futures_map):
            fq_table_name_processed = futures_map[future]
            try:
                table_pii_result_dict = future.result()
                scan_results_detail.append(table_pii_result_dict)
            except Exception as exc_future:
                logging.error(
                    f"Error processing table '{fq_table_name_processed}' in PII scan thread: {exc_future}",
                    exc_info=True,
                )
                scan_results_detail.append(
                    {
                        "full_name": fq_table_name_processed,
                        "error": str(exc_future),
                        "skipped": True,
                    }
                )

    scan_results_detail.sort(key=lambda x: x.get("full_name", ""))
    total_pii_cols_found = sum(
        r.get("pii_column_count", 0)
        for r in scan_results_detail
        if not r.get("error") and not r.get("skipped")
    )
    num_tables_with_pii = sum(
        1
        for r in scan_results_detail
        if not r.get("error") and not r.get("skipped") and r.get("has_pii")
    )
    num_tables_successfully_processed = sum(
        1 for r in scan_results_detail if not r.get("error") and not r.get("skipped")
    )

    return {
        "catalog": catalog_name,
        "schema": schema_name,
        "tables_scanned_attempted": len(tables_to_scan_summaries),
        "tables_successfully_processed": num_tables_successfully_processed,
        "tables_with_pii": num_tables_with_pii,
        "total_pii_columns": total_pii_cols_found,
        "results_detail": scan_results_detail,
    }
