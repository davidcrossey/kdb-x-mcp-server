import json
import logging
from typing import Dict, Any, List, Tuple
import kxi.query

logger = logging.getLogger(__name__)

ALLOWED_KEYS = {
    "table",
    "byCols",
    "startTS",
    "endTS"
}

def _sanitize_kwargs(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Dismiss any unknown keyword params."""
    dropped = [k for k in raw.keys() if k not in ALLOWED_KEYS]
    cleaned = {k: v for k, v in raw.items() if k in ALLOWED_KEYS}
    return cleaned, dropped

def _validate_and_normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate types and required parameters for getCountBy API.
    Returns a normalized params dict suitable for conn.exampleuda_countBy(json=params).
    Raises ValueError/TypeError on invalid input.
    """
    # table: required string
    if "table" not in params or not isinstance(params["table"], str) or not params["table"]:
        raise ValueError("Missing required param: table (str)")

    # byCols: required str or list[str]
    by_cols = params.get("byCols")
    if by_cols is None:
        raise ValueError("Missing required param: byCols (str or list[str])")
    if isinstance(by_cols, str):
        pass
    elif isinstance(by_cols, list) and all(isinstance(x, str) for x in by_cols):
        pass
    else:
        raise TypeError("byCols must be str or list[str]")

    # startTS: required string (timestamp)
    if "startTS" not in params or not isinstance(params["startTS"], str) or not params["startTS"]:
        raise ValueError("Missing required param: startTS (timestamp string)")

    # endTS: required string (timestamp)
    if "endTS" not in params or not isinstance(params["endTS"], str) or not params["endTS"]:
        raise ValueError("Missing required param: endTS (timestamp string)")

    return params


# Example
# getCountByQuery = '{"table": "dOrderReport", "byCols": "sym", "startTS": "2026-02-11T00:01:01.000000000", "endTS": "2026-02-11T23:01:01.000000000"}'

async def run_get_countby_impl(getCountByQuery: str) -> Dict[str, Any]:
    """
    getCountBy is expected to be a JSON string containing the allowed params, e.g.
    {"table": "dOrderReport", "byCols": "sym", "startTS": "2026-02-11T00:01:01.000000000", "endTS": "2026-02-11T23:01:01.000000000"}
    """
    try:
        # Parse JSON input
        try:
            raw = json.loads(getCountByQuery)
        except Exception as e:
            raise ValueError(f"query must be valid JSON: {e}") from e

        if not isinstance(raw, dict):
            raise ValueError("query JSON must be an object (dictionary)")

        # Dismiss unknown keywords
        cleaned, dropped = _sanitize_kwargs(raw)

        # Validate + normalize
        params = _validate_and_normalize_params(cleaned)

        # Initialize connection and fetch custom APIs
        conn = kxi.query.Query(data_format='application/json')
        conn.fetch_udas()

        # Call the getCountBy custom API
        result = conn.exampleuda_countBy(json=params)

        # Extract payload from response
        # API returns: {'header': {...}, 'payload': [...]}
        if 'payload' not in result:
            logger.warning(f"Unexpected response structure: {result}")
            return {"status": "error", "message": "API response missing 'payload' key"}

        payload = result['payload']
        count = len(payload)

        if count == 0:
            return {"status": "success", "data": [], "message": "No rows returned"}

        logger.info(f"Query returned {count} grouped rows.")

        response = {
            "status": "success",
            "data": payload,
            "count": count
        }

        # Optional: include dropped params for debugging
        if dropped:
            response["dropped_params"] = dropped

        return response

    except Exception as e:
        logger.error(f"CountBy query failed: {e}")
        return {"status": "error", "message": str(e)}

def register_tools(mcp_server):
    """
    Register the insights_get_countby tool with the MCP server.
    This function is called automatically during server startup.
    """
    @mcp_server.tool()
    async def insights_get_countby(query: str) -> Dict[str, Any]:
        """
        Execute a count aggregation grouped by specified columns over a time range.

        This tool calls the custom getCountBy API to count records grouped by one or more
        columns within a specified time window. It's useful for analyzing data distribution
        and frequency across categories.

        Input:
            query (str): JSON string containing parameters for the countBy operation.

            Required keys:
              - table (str): Name of the table to query
              - byCols (str|list[str]): Column(s) to group by for counting
              - startTS (str): Start timestamp in ISO format (e.g., "2026-02-11T00:01:01.000000000")
              - endTS (str): End timestamp in ISO format (e.g., "2026-02-11T23:01:01.000000000")

        Examples:
            # Count by single column
            {"table":"dOrderReport","byCols":"sym","startTS":"2026-02-11T00:01:01.000000000","endTS":"2026-02-11T23:01:01.000000000"}

            # Count by multiple columns
            {"table":"dOrderReport","byCols":["sym","orderType"],"startTS":"2026-02-11T00:01:01.000000000","endTS":"2026-02-11T23:01:01.000000000"}

        Returns:
            Dict[str, Any]: Contains:
              - status (str): "success" or "error"
              - data (list): List of dictionaries with group keys and 'cnt' field
              - count (int): Number of grouped rows returned
              - message (str): Optional status message
              - dropped_params (list): Optional list of ignored parameters
        """
        return await run_get_countby_impl(getCountByQuery=query)

    return ['insights_get_countby']