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
    "bucket",
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
    Validate types and required parameters for getMidrateOhlc API.
    Returns a normalized params dict suitable for conn.smbcuda_midrateOhlc(json=params).
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

    # bucket: required string (timespan, e.g. "0D00:01:00.000000000" for 1 minute)
    if "bucket" not in params or not isinstance(params["bucket"], str) or not params["bucket"]:
        raise ValueError("Missing required param: bucket (timespan string, e.g. '0D00:01:00.000000000')")

    # startTS: required string (timestamp)
    if "startTS" not in params or not isinstance(params["startTS"], str) or not params["startTS"]:
        raise ValueError("Missing required param: startTS (timestamp string)")

    # endTS: required string (timestamp)
    if "endTS" not in params or not isinstance(params["endTS"], str) or not params["endTS"]:
        raise ValueError("Missing required param: endTS (timestamp string)")

    return params


async def run_get_midrate_ohlc_impl(getMidrateOhlcQuery: str) -> Dict[str, Any]:
    """
    getMidrateOhlc is expected to be a JSON string containing the allowed params, e.g.
    {"table": "dfxMidRateTOB", "bucket": "0D00:01:00.000000000", "startTS": "2026-03-05T05:00:01.000000000", "endTS": "2026-03-05T06:00:01.000000000"}
    """
    try:
        # Parse JSON input
        try:
            raw = json.loads(getMidrateOhlcQuery)
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

        # Call the midrateOhlc custom API
        result = conn.smbcuda_midrateOhlc(json=params)

        # Extract payload from response
        if 'payload' not in result:
            logger.warning(f"Unexpected response structure: {result}")
            return {"status": "error", "message": "API response missing 'payload' key"}

        payload = result['payload']
        count = len(payload)

        if count == 0:
            return {"status": "success", "data": [], "message": "No rows returned"}

        logger.info(f"Query returned {count} OHLC rows.")

        response = {
            "status": "success",
            "data": encode(payload),
            "count": count
        }

        if dropped:
            response["dropped_params"] = dropped

        return response

    except Exception as e:
        logger.error(f"MidrateOhlc query failed: {e}")
        return {"status": "error", "message": str(e)}

def register_tools(mcp_server):
    """
    Register the insights_get_midrate_ohlc tool with the MCP server.
    This function is called automatically during server startup.
    """
    @mcp_server.tool()
    @track_size(tracker, "insights_get_midrate_ohlc")
    async def insights_get_midrate_ohlc(query: str) -> Dict[str, Any]:
        """
        Compute OHLC candles for FX mid rate data (dfxMidRateTOB) over a time range.

        This tool calls the custom midrateOhlc UDA to aggregate high-frequency tick data
        into time-bucketed OHLC candles per symbol. Each row contains open, high, low,
        close mid rate, average/min/max spread, average bid/ask sizes, and tick count.
        Use this instead of fetching raw tick data to drastically reduce data volume.

        Input:
            query (str): JSON string containing parameters for the midrateOhlc operation.

            Required keys:
              - table (str): Name of the table to query (typically "dfxMidRateTOB")
              - bucket (str): Time bucket size as a timespan string (e.g. "0D00:01:00.000000000" for 1 minute, "0D00:05:00.000000000" for 5 minutes)
              - startTS (str): Start timestamp in ISO format (e.g., "2026-03-05T05:00:01.000000000")
              - endTS (str): End timestamp in ISO format (e.g., "2026-03-05T06:00:01.000000000")

            Optional keys:
              - syms (str|list[str]): Symbol(s) to filter on (e.g. "USD/JPY" or ["USD/JPY","EUR/USD"]). Omit to return all symbols.

        Examples:
            # 1-minute OHLC for all syms
            {"table":"dfxMidRateTOB","bucket":"0D00:01:00.000000000","startTS":"2026-03-05T05:00:01.000000000","endTS":"2026-03-05T06:00:01.000000000"}

            # 5-minute OHLC for USD/JPY only
            {"table":"dfxMidRateTOB","syms":"USD/JPY","bucket":"0D00:05:00.000000000","startTS":"2026-03-05T05:00:01.000000000","endTS":"2026-03-05T06:00:01.000000000"}

        Returns:
            Dict[str, Any]: Contains:
              - status (str): "success" or "error"
              - data (list): List of OHLC rows with fields: timeBucket, sym, open, high, low, close, avgMid, avgSpread, minSpread, maxSpread, avgBidSize, avgAskSize, tickCount
              - count (int): Number of rows returned
              - message (str): Optional status message
              - dropped_params (list): Optional list of ignored parameters
        """
        return await run_get_midrate_ohlc_impl(getMidrateOhlcQuery=query)

    return ['insights_get_midrate_ohlc']
