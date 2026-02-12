import json
import logging
from typing import Dict, Any, List, Optional
import kxi.query

logger = logging.getLogger(__name__)

MAX_ROWS_RETURNED = 1000

ALLOWED_KEYS = {
    "table",
    "byCols",
    "startTS",
    "endDTS",
    "limit"
}

def _sanitize_kwargs(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Dismiss any unknown keyword params."""
    dropped = [k for k in raw.keys() if k not in ALLOWED_KEYS]
    cleaned = {k: v for k, v in raw.items() if k in ALLOWED_KEYS}
    return cleaned, dropped

def _validate_and_normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate types/restrictions from your spec.
    Returns a normalized params dict suitable for conn.get_data(table, **kwargs).
    Raises ValueError/TypeError on invalid input.
    """
    if "table" not in params or not isinstance(params["table"], str) or not params["table"]:
        raise ValueError("Missing required param: table (str)")

    # limit: int | list[int]
    limit = params.get("limit")
    if limit is not None:
        if isinstance(limit, int):
            params["limit"] = max(-MAX_ROWS_RETURNED, min(limit, MAX_ROWS_RETURNED))
        elif isinstance(limit, list) and all(isinstance(x, int) and x for x in limit):
            params["limit"] = [ max(-MAX_ROWS_RETURNED, min(x, MAX_ROWS_RETURNED)) for x in limit ]
        else:
            raise TypeError("limit must be int or list[int]")
    else:
        # Optional: default to MAX_ROWS_RETURNED to be safe
        params["limit"] = MAX_ROWS_RETURNED

    return params


# Example
# getCountByQuery = '"table": "dOrderReport", "byCols": "sym", "startTS": "2026-02-11T00:01:01.000000000", "endTS": "2026-02-11T23:01:01.000000000"'

# Pick descriptive tool name
async def run_get_countby_impl(getCountByQuery: str) -> Dict[str, Any]:
    """
    getCountBy is expected to be a JSON string containing the allowed params, e.g.
    {"table": "dOrderReport", "byCols": "sym", "startTS": "2026-02-11T00:01:01.000000000", "endTS": "2026-02-11T23:01:01.000000000"}
    """
    try:
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

        conn = kxi.query.Query(data_format='application/json')
        conn.fetch_udas()

        data = conn.exampleuda_countBy(json=params)

        result = {'rowCount': len(data), 'data': data}
        total = int(result['rowCount'])

        if 0==total:
            return {"status": "success", "data": [], "message": "No rows returned"}

        rows = result['data']
        if total > MAX_ROWS_RETURNED:
            logger.info(f"Table has {total} rows. Query returned truncated data to {MAX_ROWS_RETURNED} rows.")
            return {
                "status": "success",
                "data": rows,
                "message": f"Showing first {MAX_ROWS_RETURNED} of {total} rows",
            }

        logger.info(f"Query returned {total} rows.")

        # Optional: include dropped params for debugging (remove if you don't want it in responses)
        return {
            "status": "success",
            "data": rows,
            "dropped_params": dropped,
        }
            
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return {"status": "error", "message": str(e)}

def register_tools(mcp_server):
    """
    Register your tool with the MCP server.
    This function is called automatically during server startup.
    """
    @mcp_server.tool()
    async def insights_get_countby(query: str) -> Dict[str, Any]:
        """    
        Detailed explanation of the tool's functionality, including:
        - What data it processes
        - What algorithms or methods it uses
        - Expected use cases
        
        Args:
            param1 (str): Description of the first parameter
            param2 (int): Description of the second parameter  
            param3 (Optional[List[str]]): Description of optional parameter
            
        Returns:
            Dict[str, Any]: Description of return value structure
        """        
        return await run_get_countby_impl(getCountByQuery=query)
    return ['insights_get_countby']