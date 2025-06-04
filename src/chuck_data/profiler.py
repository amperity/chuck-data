import logging
import datetime
import time
import base64
import json


def list_tables(client, warehouse_id):
    """
    Lists all tables in Unity Catalog using a SQL query.

    Args:
        client: DatabricksAPIClient instance
        warehouse_id: ID of the SQL warehouse

    Returns:
        List of dictionaries with table_name, catalog_name, and schema_name
    """
    print("Listing tables in Unity Catalog...", warehouse_id)
    sql_text = "SELECT table_name, catalog_name, schema_name FROM system.information_schema.tables;"
    data = {
        "on_wait_timeout": "CONTINUE",
        "statement": sql_text,
        "wait_timeout": "30s",
        "warehouse_id": warehouse_id,
    }

    # Submit the SQL statement
    response = client.post("/api/2.0/sql/statements", data)
    statement_id = response.get("statement_id")

    # Poll until complete
    state = None
    while True:
        status = client.get(f"/api/2.0/sql/statements/{statement_id}")
        state = status.get("status", {}).get("state", status.get("state"))
        if state not in ["PENDING", "RUNNING"]:
            break
        time.sleep(1)

    # Get results
    if state != "SUCCEEDED":
        return []

    # Parse results
    result = status.get("result", {})
    data_array = result.get("data", [])

    tables = []
    for row in data_array:
        tables.append(
            {"table_name": row[0], "catalog_name": row[1], "schema_name": row[2]}
        )

    return tables


def get_table_schema(client, warehouse_id, catalog_name, schema_name, table_name):
    """
    Retrieves the extended schema of a specified table.

    Args:
        client: DatabricksAPIClient instance
        warehouse_id: ID of the SQL warehouse
        catalog_name: Catalog name
        schema_name: Schema name
        table_name: Table name

    Returns:
        List of schema details
    """
    sql_text = f"DESCRIBE EXTENDED {catalog_name}.{schema_name}.{table_name}"
    data = {"warehouse_id": warehouse_id, "catalog": catalog_name, "sql_text": sql_text}

    # Submit the SQL statement
    response = client.post("/api/2.0/sql/statements", data)
    statement_id = response.get("statement_id")

    # Poll until complete
    state = None
    while True:
        status = client.get(f"/api/2.0/sql/statements/{statement_id}")
        state = status.get("status", {}).get("state", status.get("state"))
        if state not in ["PENDING", "RUNNING"]:
            break
        time.sleep(1)

    # Get results
    if state != "SUCCEEDED":
        return []

    # Parse results
    result = status.get("result", {})
    data_array = result.get("data", [])

    # Format schema info
    schema = []
    for row in data_array:
        schema.append(
            {
                "col_name": row[0],
                "data_type": row[1],
                "comment": row[2] if len(row) > 2 else "",
            }
        )

    return schema


def get_sample_data(client, warehouse_id, catalog_name, schema_name, table_name):
    """
    Retrieves sample data (first 10 rows) from the specified table.

    Args:
        client: DatabricksAPIClient instance
        warehouse_id: ID of the SQL warehouse
        catalog_name: Catalog name
        schema_name: Schema name
        table_name: Table name

    Returns:
        Dictionary with column_names and rows
    """
    sql_text = f"SELECT * FROM {catalog_name}.{schema_name}.{table_name} LIMIT 10"
    data = {"warehouse_id": warehouse_id, "catalog": catalog_name, "sql_text": sql_text}

    # Submit the SQL statement
    response = client.post("/api/2.0/sql/statements", data)
    statement_id = response.get("statement_id")

    # Poll until complete
    state = None
    while True:
        status = client.get(f"/api/2.0/sql/statements/{statement_id}")
        state = status.get("status", {}).get("state", status.get("state"))
        if state not in ["PENDING", "RUNNING"]:
            break
        time.sleep(1)

    # Get results
    if state != "SUCCEEDED":
        return []

    # Parse results
    result = status.get("result", {})
    column_names = [col["name"] for col in result.get("schema", [])]
    sample_rows = []

    for row in result.get("data", []):
        sample_row = {}
        for i, col_name in enumerate(column_names):
            if i < len(row):
                sample_row[col_name] = row[i]
            else:
                sample_row[col_name] = None
        sample_rows.append(sample_row)

    return {"column_names": column_names, "rows": sample_rows}


def query_llm(client, endpoint_name, input_data):
    """
    Queries the LLM via the Serving Endpoints API.

    Args:
        client: DatabricksAPIClient instance
        endpoint_name: Name of the serving endpoint
        input_data: Data to send to the LLM

    Returns:
        Response from the LLM
    """
    endpoint = f"/api/2.0/serving-endpoints/{endpoint_name}/invocations"

    # Format input for the LLM
    # This format may need adjustment based on model requirements
    request_data = {
        "inputs": [
            {"schema": input_data["schema"], "sample_data": input_data["sample_data"]}
        ]
    }

    response = client.post(endpoint, request_data)
    return response


def generate_manifest(table_info, schema, sample_data, pii_tags):
    """
    Generates a JSON manifest with profiling results.

    Args:
        table_info: Dictionary with table_name, catalog_name, schema_name
        schema: Table schema information
        sample_data: Sample data from the table
        pii_tags: PII tags from LLM response

    Returns:
        Dictionary representing the manifest
    """
    manifest = {
        "table": {
            "catalog_name": table_info["catalog_name"],
            "schema_name": table_info["schema_name"],
            "table_name": table_info["table_name"],
        },
        "schema": schema,
        "pii_tags": pii_tags,
        "profiling_timestamp": datetime.datetime.now().isoformat(),
    }

    return manifest


def store_manifest(client, manifest_path, manifest):
    """
    Stores the manifest in DBFS.

    Args:
        client: DatabricksAPIClient instance
        manifest_path: Path in DBFS
        manifest: Dictionary to store

    Returns:
        True if successful, False otherwise
    """
    # DBFS API endpoint for file upload
    endpoint = "/api/2.0/dbfs/put"

    # Convert manifest to JSON string
    manifest_json = json.dumps(manifest, indent=2)

    # Prepare the request with file content and path
    request_data = {
        "path": manifest_path,
        "contents": base64.b64encode(manifest_json.encode()).decode(),
        "overwrite": True,
    }

    try:
        client.post(endpoint, request_data)
        return True
    except Exception as e:
        logging.error(f"Failed to store manifest: {e}")
        return False


def profile_table(client, warehouse_id, endpoint_name, table_info=None):
    """
    Main function to orchestrate the profiling process.

    Args:
        client: DatabricksAPIClient instance
        warehouse_id: ID of the SQL warehouse
        endpoint_name: Name of the serving endpoint
        table_info: Optional dictionary with table_name, catalog_name, schema_name
                   If None, the first table will be used

    Returns:
        Path to the stored manifest, or None if profiling failed
    """
    try:
        # Step 1: List tables if no specific table provided
        if table_info is None:
            tables = list_tables(client, warehouse_id)
            if not tables:
                logging.error("No tables found")
                return None

            # Select the first table
            table_info = tables[0]

        # Step 2: Get schema and sample data
        schema = get_table_schema(
            client,
            warehouse_id,
            table_info["catalog_name"],
            table_info["schema_name"],
            table_info["table_name"],
        )

        if not schema:
            logging.error("Failed to retrieve schema")
            return None

        sample_data = get_sample_data(
            client,
            warehouse_id,
            table_info["catalog_name"],
            table_info["schema_name"],
            table_info["table_name"],
        )

        if not sample_data:
            logging.error("Failed to retrieve sample data")
            return None

        # Step 3: Prepare input for LLM
        input_data = {"schema": schema, "sample_data": sample_data}

        # Step 4: Query LLM
        llm_response = query_llm(client, endpoint_name, input_data)

        # Extract PII tags from response (format may vary based on LLM)
        # This is a simplified extraction - adjust based on actual response format
        pii_tags = llm_response.get("predictions", [])[0].get("pii_tags", [])

        # Step 5: Generate manifest
        manifest = generate_manifest(table_info, schema, sample_data, pii_tags)

        # Step 6: Store manifest in DBFS
        manifest_path = f"/chuck/manifests/{table_info['table_name']}_manifest.json"
        success = store_manifest(client, manifest_path, manifest)

        if success:
            return manifest_path
        else:
            return None

    except Exception as e:
        logging.error(f"Error during profiling: {e}")
        return None
