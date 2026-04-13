import importlib
import logging
import pkgutil
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

logger = logging.getLogger(__name__)

# Read-only mode flag - set before loading tools
read_only_mode = False

# Tools that are safe in read-only mode (no data modification)
READ_ONLY_TOOLS = {
    # String operations
    "get",
    # Hash operations
    "hget", "hgetall", "hexists", "get_vector_from_hash",
    # List operations
    "lrange", "llen",
    # Set operations
    "smembers",
    # Sorted set operations
    "zrange",
    # Stream operations
    "xrange",
    # JSON operations
    "json_get",
    # Server management
    "dbsize", "info", "client_list",
    # Connection management (read-only operations)
    "list_connections", "get_connection",
    # Search/query operations
    "get_indexes", "get_index_info", "get_indexed_keys_number",
    "vector_search_hash", "text_search", "get_index_dialect",
    # Misc read operations
    "type", "scan_keys", "scan_all_keys",
}


def load_tools(read_only: bool = False):
    """Load tools from the tools package.
    
    Args:
        read_only: If True, only read-only tools will be available.
    """
    global read_only_mode
    read_only_mode = read_only
    
    import src.tools as tools_pkg

    for _, module_name, _ in pkgutil.iter_modules(tools_pkg.__path__):
        importlib.import_module(f"src.tools.{module_name}")
    
    total_tools = len(mcp._tool_manager._tools) if hasattr(mcp, '_tool_manager') else 0
    logger.info(f"Loaded {total_tools} tools (read_only={read_only})")
    
    # Filter out write tools if in read-only mode
    if read_only_mode:
        _filter_write_tools()
        remaining_tools = len(mcp._tool_manager._tools) if hasattr(mcp, '_tool_manager') else 0
        logger.info(f"Read-only mode: filtered to {remaining_tools} tools")


def _filter_write_tools():
    """Remove write tools from the MCP server when in read-only mode."""
    # Get the internal tools dictionary from FastMCP
    if hasattr(mcp, '_tool_manager') and hasattr(mcp._tool_manager, '_tools'):
        tools_dict = mcp._tool_manager._tools
        write_tools = [name for name in tools_dict.keys() if name not in READ_ONLY_TOOLS]
        logger.debug(f"Removing {len(write_tools)} write tools: {write_tools}")
        for tool_name in write_tools:
            del tools_dict[tool_name]
    else:
        logger.warning("Could not access _tool_manager._tools for filtering")


# Initialize FastMCP server
mcp = FastMCP(
    "Redis MCP Server",
    dependencies=["redis", "dotenv", "numpy"],
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)
