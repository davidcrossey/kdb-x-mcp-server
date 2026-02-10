
import logging

logger = logging.getLogger(__name__)

def insights_sql_query_guidance_impl() -> str:
    path = "src/mcp_server/resources/insights_sql_query_guidance.txt"
    with open(path, 'r', encoding='utf-8') as file:
        return file.read()

def register_resources(mcp_server):
    @mcp_server.resource("file://guidance/insights-sql-queries")
    async def insights_sql_query_guidance() -> str:
        """
        Provides guidance when using SQL select statements with the insights_run_sql_query tool.

        Returns:
            str: Details and examples on supported select statement when using the sql tool.
        """
        return insights_sql_query_guidance_impl()
    return ['file://guidance/insights-sql-queries']