
import logging

logger = logging.getLogger(__name__)

def kdbx_get_data_guidance_impl() -> str:
    path = "src/mcp_server/resources/kdbx_get_data_guidance.txt"
    with open(path, 'r', encoding='utf-8') as file:
        return file.read()

def register_resources(mcp_server):
    @mcp_server.resource("file://guidance/kdbx-get-data")
    async def kdbx_get_data_guidance() -> str:
        """
        Provides guidance when using get_data API with the kdbx_get_data tool.

        Returns:
            str: Details and examples on supported parameters when using the api tool.
        """
        return kdbx_get_data_guidance_impl()
    return ['file://guidance/kdbx-get-data']