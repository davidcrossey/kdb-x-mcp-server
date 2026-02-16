import logging
import json
from typing import Dict, Any, List, Union, Tuple
import kxi.query
from mcp_server.stats.mcp_size_tracker import SizeTracker, track_size

logger = logging.getLogger(__name__)
MAX_ROWS_RETURNED = 1000

tracker = SizeTracker("insights_size_log.json")

# ----------------------------
# Allowed params + validation
# ----------------------------
Triad = List[Any]  # ["within", "qual", [0,2]]
FilterType = List[Triad]
LabelsType = Dict[str, str]

GroupByType = Union[str, List[str]]
SortColumnsType = Union[str, List[str]]

AggTriplet = List[str]  # ["assignname", "agg", "column"]
AggregationsType = Union[str, List[str], List[AggTriplet]]

LimitType = Union[int, List[int]]

ALLOWED_KEYS = {
    "table",
    "start_time",
    "end_time",
    "input_timezone",
    "output_timezone",
    "filter",
    "group_by",
    "aggregations",
    "fill",
    "temporality",
    "slice",
    "sort_columns",
    "labels",
    "limit",
}

ALLOWED_FILL = {"forward", "zero"}
ALLOWED_TEMPORALITY = {"slice", "snapshot"}


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

    # fill restriction
    fill = params.get("fill")
    if fill is not None:
        if not isinstance(fill, str) or fill not in ALLOWED_FILL:
            raise ValueError("fill must be 'forward' or 'zero'")

    # temporality restriction
    temporality = params.get("temporality")
    if temporality is not None:
        if not isinstance(temporality, str) or temporality not in ALLOWED_TEMPORALITY:
            raise ValueError("temporality must be 'slice' or 'snapshot'")

    # slice requirement when temporality='slice'
    if temporality == "slice":
        sl = params.get("slice")
        if sl is None or not (isinstance(sl, list) and all(isinstance(x, str) for x in sl) and len(sl) > 0):
            raise ValueError("slice must be provided as list[str] when temporality='slice'")

    # filter: list of triads
    flt = params.get("filter")
    if flt is not None:
        if not isinstance(flt, list):
            raise TypeError("filter must be a list")
        for item in flt:
            if not (isinstance(item, list) and len(item) == 3):
                raise ValueError("Each filter condition must be a 3-item list: ['function','column','parameter']")

    # group_by: str or list[str]
    gb = params.get("group_by")
    if gb is not None:
        if isinstance(gb, str):
            pass
        elif isinstance(gb, list) and all(isinstance(x, str) for x in gb):
            pass
        else:
            raise TypeError("group_by must be str or list[str]")

    # sort_columns: str or list[str]
    sc = params.get("sort_columns")
    if sc is not None:
        if isinstance(sc, str):
            pass
        elif isinstance(sc, list) and all(isinstance(x, str) for x in sc):
            pass
        else:
            raise TypeError("sort_columns must be str or list[str]")

    # aggregations: str | list[str] | list[list[str]] (3 strings each)
    aggs = params.get("aggregations")
    if aggs is not None:
        if isinstance(aggs, str):
            pass
        elif isinstance(aggs, list):
            if all(isinstance(x, str) for x in aggs):
                pass
            elif all(isinstance(x, list) for x in aggs):
                for trip in aggs:
                    if not (len(trip) == 3 and all(isinstance(s, str) for s in trip)):
                        raise TypeError("aggregations triplets must be ['assignname','agg','column'] (3 strings)")
            else:
                raise TypeError("aggregations must be str, list[str], or list of 3-string lists")
        else:
            raise TypeError("aggregations must be str, list[str], or list of 3-string lists")

    # labels: dict[str,str]
    labels = params.get("labels")
    if labels is not None:
        if not isinstance(labels, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in labels.items()):
            raise TypeError("labels must be dict[str, str]")

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
# getDataQuery = '{"table":"dOrderReport","start_time": "2025.11.20","end_time": "2025.11.31"}'

# ----------------------------
# Core implementation
# ----------------------------
async def run_get_data_impl(getDataQuery: str) -> Dict[str, Any]:
    """
    getDataQuery is expected to be a JSON string containing the allowed params, e.g.
    {"table":"my_table","group_by":["sensorID","qual"]}
    """
    try:
        # Parse JSON input
        try:
            raw = json.loads(getDataQuery)
        except Exception as e:
            raise ValueError(f"query must be valid JSON: {e}") from e

        if not isinstance(raw, dict):
            raise ValueError("query JSON must be an object (dictionary)")

        # Dismiss unknown keywords
        cleaned, dropped = _sanitize_kwargs(raw)

        # Validate + normalize
        params = _validate_and_normalize_params(cleaned)

        # Extract table and forward kwargs
        table = params.pop("table")

        # NOTE: conn.get_data appears to be synchronous in most usages.
        # If it is actually async in your environment, change this line to: rows = await conn.get_data(...)
        conn = kxi.query.Query(data_format='application/json')
        data = conn.get_data(table, **params)

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
    @mcp_server.tool()
    @track_size(tracker, "insights_get_data")
    async def insights_get_data(query: str) -> Dict[str, Any]:
        """
        Execute an API call and return structured results only to be used on kdb and not on kdbai.

        Managing token limits:
            Limit tables to smbcpoc assembly (9 tables); always use start_time/end_time (default to 'today'); always use limit (default -10 for the last 10 N records); encourage use of filters/group_by/aggregations to reduce result size. Only pull back specific columns—avoid SELECT * style queries.

        Input:
            query (str): JSON string containing parameters for get_data.

            Allowed keys (unknown keys are ignored):
              - table (str) [required]
              - start_time (str|datetime) [optional; 15 minutes prior to the time that the get_data call is made]
              - end_time (str|datetime) [optional; The time that the get_data function is called]
              - input_timezone (str) [default 'UTC']
              - output_timezone (str) [default 'UTC']
              - filter (list) [optional] e.g. [["within","qual",[0,2]]]
              - group_by (str|list[str]) [optional]
              - aggregations (str|list[str]|list[list[str]]) [optional; triplets are 3 strings]
              - fill (str) [optional; must be 'forward' or 'zero']
              - temporality (str) [optional; must be 'slice' or 'snapshot']
              - slice (list[str]) [required if temporality='slice']
              - sort_columns (str|list[str]) [optional]
              - labels (dict[str,str]) [optional]
              - limit (int|list[int]) [optional; int is clamped to MAX_ROWS_RETURNED]

        Filter syntax: [["function", "column", parameter]]
        Aggregation syntax: [["assign_name", "function", "column"]]

        Examples:
            # Get last 5 rows from a table
            {"table":"dOrderReport","start_time":"2026.02.08","end_time":"2026.02.09","limit":-5}
            
            # Group sensor data by ID and quality
            {"table":"dOrderReport","start_time":"2026.02.08","end_time":"2026.02.09","limit":-5,"group_by":["sensorID","qual"]}
            
            # Filter for quality values between 0 and 2
            {"table":"dOrderReport","start_time":"2026.02.08","end_time":"2026.02.09","filter":[["within","qual",[0,2]]]}
            
            # Time-range query with row count aggregation
            {"table":"dOrderReport","start_time":"2026.02.08","end_time":"2026.02.09","aggregations":[["cnt","count","time"]]}

        For API syntax and examples, see: file://guidance/insights-get-data

        Returns:
            Dict[str, Any]: Query execution results.
        """
        return await run_get_data_impl(getDataQuery=query)

    return ["insights_get_data"]
