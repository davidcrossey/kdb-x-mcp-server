import logging
from typing import Dict, Any
import kxi.query

logger = logging.getLogger(__name__)

# ----------------------------
# Core implementation
# ----------------------------
async def run_get_meta_impl() -> Dict[str, Any]:

    try:
        conn = kxi.query.Query(data_format='application/json')
        data = conn.get_meta()

        ## todo - limit data result to smbcpoc asm to help with context size??

        result = {'rowCount': len(data), 'data': data}
        total = int(result['rowCount'])
        if 0==total:
            return {"status": "success", "data": [], "message": "No rows returned"}
        rows = result['data']
        logger.info(f"Query returned {total} rows.")

        return {
            "status": "success",
            "data": rows
        }

    except Exception as e:
        logger.error(f"Query failed: {e}")
        return {"status": "error", "message": str(e)}


def register_tools(mcp_server):
    @mcp_server.tool()
    async def insights_get_meta() -> Dict[str, Any]:
        """
        Execute an API call and return structured results only to be used on kdb and not on kdbai.

        Input:
            None

        Returns:
            Dict[str, Any]: Query execution results.
        """
        return await run_get_meta_impl()

    return ["insights_get_meta"]
