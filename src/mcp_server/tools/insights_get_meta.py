import logging
from typing import Dict, Any, Optional
import kxi.query

logger = logging.getLogger(__name__)

# ----------------------------
# Core implementation
# ----------------------------
async def run_get_meta_impl(key: str = "assembly", tbl: Optional[str] = None) -> Dict[str, Any]:

    try:
        conn = kxi.query.Query(data_format='application/json')
        data = conn.get_meta()

        # Filter data by key
        valid_keys = ['rc', 'dap', 'api', 'agg', 'assembly', 'schema']
        if key not in valid_keys:
            return {
                "status": "error",
                "message": f"Invalid key '{key}'. Must be one of: {', '.join(valid_keys)}"
            }

        # Extract the specified key from data
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return {
                "status": "error",
                "message": f"Key '{key}' not found in metadata"
            }

        # If key is schema, require table name
        if key == "schema":
            if not isinstance(data, list):
                return {
                    "status": "error",
                    "message": "Schema data is not in expected list format"
                }

            # Extract list of available tables first (before filtering)
            available_tables = [item.get('table') for item in data if item.get('table')]

            if not tbl or tbl.strip() == '':
                return {
                    "status": "error",
                    "message": "Table name (tbl) must be provided when querying schema",
                    "available_tables": available_tables
                }

            # Filter by table name
            data = [item for item in data if item.get('table') == tbl]
            if not data:
                return {
                    "status": "error",
                    "message": f"No schema found for table: {tbl}",
                    "available_tables": available_tables
                }

        result = {'rowCount': len(data) if isinstance(data, list) else 1, 'data': data}
        total = int(result['rowCount'])
        if 0==total:
            return {"status": "success", "data": [], "message": "No rows returned"}
        rows = result['data']
        logger.info(f"Query returned {total} rows for key: {key}{f', table: {tbl}' if tbl else ''}.")

        return {
            "status": "success",
            "data": rows
        }

    except Exception as e:
        logger.error(f"Query failed: {e}")
        return {"status": "error", "message": str(e)}


def register_tools(mcp_server):
    @mcp_server.tool()
    async def insights_get_meta(key: str = "assembly", tbl: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute an API call and return structured metadata only to be used on kdb and not on kdbai.

        Input:
            key (optional, default="assembly"): Filter metadata by key.
                Valid values: rc, dap, api, agg, assembly, schema
            tbl (optional): Table name to filter schema results. Only used when key="schema".
                When provided with key="schema", returns only the schema for the specified table.

        Examples:
            - insights_get_meta() - Returns assembly metadata (default)
            - insights_get_meta(key="schema") - Returns a list of tables to use with tbl parameter
            - insights_get_meta(key="schema", tbl="blp_mktnews_derived_tickers") - Returns schema for specific table
            - insights_get_meta(key="api") - Returns API metadata
            - insights_get_meta(key="dap") - Returns data access policy metadata

        Returns:
            Dict[str, Any]: Query execution results with filtered metadata.
        """
        return await run_get_meta_impl(key, tbl)

    return ["insights_get_meta"]
