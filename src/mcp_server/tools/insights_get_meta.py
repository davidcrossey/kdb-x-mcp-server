import logging
from typing import Dict, Any, Optional
import kxi.query
from mcp_server.stats.mcp_size_tracker import SizeTracker, track_size
from toon_format import encode

logger = logging.getLogger(__name__)

tracker = SizeTracker("insights_size_log.json")

VALID_KEYS = frozenset({'rc', 'dap', 'api', 'agg', 'assembly', 'schema'})

# ----------------------------
# Core implementation
# ----------------------------
async def run_get_meta_impl(key: str = "assembly", tbl: Optional[str] = None) -> Dict[str, Any]:

    # Validate inputs before making the network call
    if key not in VALID_KEYS:
        return {
            "status": "error",
            "message": f"Invalid key '{key}'. Must be one of: {', '.join(VALID_KEYS)}"
        }

    try:
        conn = kxi.query.Query(data_format='application/json')
        data = conn.get_meta()

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
            available_tables = [t for item in data if (t := item.get('table'))]

            if not tbl or not tbl.strip():
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

        rows = data if isinstance(data, list) else [data]
        if not rows:
            return {"status": "success", "data": [], "message": "No rows returned"}

        logger.info(f"Query returned {len(rows)} rows for key: {key}{f', table: {tbl}' if tbl else ''}.")

        return {
            "status": "success",
            "data": encode(rows)
        }

    except Exception as e:
        logger.exception(f"Query failed for key={key}, tbl={tbl}")
        return {"status": "error", "message": str(e)}


def register_tools(mcp_server):
    @mcp_server.tool()
    @track_size(tracker, "insights_get_meta")
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
