import json
import logging
from typing import Dict, Any, List, Tuple
import kxi.query
from mcp_server.stats import tracker, track_size
from toon_format import encode

logger = logging.getLogger(__name__)

ALLOWED_KEYS = {
    "table",
    "syms",
    "groupByCols",
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
    Validate types and required parameters for getQuoteSpread API.
    Returns a normalized params dict suitable for conn.smbcuda_quoteSpread(json=params).
    Raises ValueError/TypeError on invalid input.
    """
    # table: required string
    if "table" not in params or not isinstance(params["table"], str) or not params["table"]:
        raise ValueError("Missing required param: table (str)")

    # syms: optional str or list[str]
    syms = params.get("syms")
    if syms is not None:
        if isinstance(syms, str):
            pass
        elif isinstance(syms, list) and all(isinstance(x, str) for x in syms):
            pass
        else:
            raise TypeError("syms must be str or list[str]")

    # groupByCols: optional str or list[str]
    group_by_cols = params.get("groupByCols")
    if group_by_cols is not None:
        if isinstance(group_by_cols, str):
            pass
        elif isinstance(group_by_cols, list) and all(isinstance(x, str) for x in group_by_cols):
            pass
        else:
            raise TypeError("groupByCols must be str or list[str]")

    # startTS: required string (timestamp)
    if "startTS" not in params or not isinstance(params["startTS"], str) or not params["startTS"]:
        raise ValueError("Missing required param: startTS (timestamp string)")

    # endTS: required string (timestamp)
    if "endTS" not in params or not isinstance(params["endTS"], str) or not params["endTS"]:
        raise ValueError("Missing required param: endTS (timestamp string)")

    return params


async def run_get_quote_spread_impl(getQuoteSpreadQuery: str) -> Dict[str, Any]:
    """
    getQuoteSpread is expected to be a JSON string containing the allowed params, e.g.
    {"table": "dfxQuote", "startTS": "2026-03-05T05:00:01.000000000", "endTS": "2026-03-05T06:00:01.000000000"}
    """
    try:
        # Parse JSON input
        try:
            raw = json.loads(getQuoteSpreadQuery)
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
        conn.fetch_custom_apis()

        # Call the quoteSpread custom API
        result = conn.smbcuda_quoteSpread(json=params)

        # Extract payload from response
        if 'payload' not in result:
            logger.warning(f"Unexpected response structure: {result}")
            return {"status": "error", "message": "API response missing 'payload' key"}

        payload = result['payload']
        count = len(payload)

        if count == 0:
            return {"status": "success", "data": [], "message": "No rows returned"}

        logger.info(f"Query returned {count} spread stat rows.")

        response = {
            "status": "success",
            "data": encode(payload),
            "count": count
        }

        if dropped:
            response["dropped_params"] = dropped

        return response

    except Exception as e:
        logger.error(f"QuoteSpread query failed: {e}")
        return {"status": "error", "message": str(e)}

def register_tools(mcp_server):
    """
    Register the insights_get_quote_spread tool with the MCP server.
    This function is called automatically during server startup.
    """
    @mcp_server.tool()
    @track_size(tracker, "insights_get_quote_spread")
    async def insights_get_quote_spread(query: str) -> Dict[str, Any]:
        """
        Compute spread and liquidity statistics for FX quote data (dfxQuote) over a time range.

        This tool calls the custom quoteSpread UDA to aggregate the high-volume quote stream
        into compact spread statistics per symbol (and optional grouping columns such as src
        or tenor). Each row contains quote count, avg/min/max spread, volume-weighted average
        spread (VWAS), and average bid/ask sizes. Use this instead of fetching raw quote data
        to drastically reduce data volume.

        Input:
            query (str): JSON string containing parameters for the quoteSpread operation.

            Required keys:
              - table (str): Name of the table to query (typically "dfxQuote")
              - startTS (str): Start timestamp in ISO format (e.g., "2026-03-05T05:00:01.000000000")
              - endTS (str): End timestamp in ISO format (e.g., "2026-03-05T06:00:01.000000000")

            Optional keys:
              - syms (str|list[str]): Symbol(s) to filter on (e.g. "USD/JPY" or ["USD/JPY","EUR/USD"]). Omit to return all symbols.
              - groupByCols (str|list[str]): Additional column(s) to group by beyond sym (e.g. "src", ["src","tenor"]). Omit to group by sym only.

        Examples:
            # Spread stats for all syms, grouped by sym only
            {"table":"dfxQuote","startTS":"2026-03-05T05:00:01.000000000","endTS":"2026-03-05T06:00:01.000000000"}

            # Spread stats for USD/JPY, broken down by src and tenor
            {"table":"dfxQuote","syms":"USD/JPY","groupByCols":["src","tenor"],"startTS":"2026-03-05T05:00:01.000000000","endTS":"2026-03-05T06:00:01.000000000"}

        Returns:
            Dict[str, Any]: Contains:
              - status (str): "success" or "error"
              - data (list): List of rows with fields: sym, (groupByCols...), quoteCount, avgSpread, minSpread, maxSpread, vwas, avgBidSize, avgAskSize, avgBid, avgAsk
              - count (int): Number of rows returned
              - message (str): Optional status message
              - dropped_params (list): Optional list of ignored parameters
        """
        return await run_get_quote_spread_impl(getQuoteSpreadQuery=query)

    return ['insights_get_quote_spread']
