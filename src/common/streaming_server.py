# Copyright Redis Contributors
# SPDX-License-Identifier: MIT

import logging


async def serve_streaming(
    host: str = '0.0.0.0',
    port: int = 8000,
) -> None:
    """Serve the MCP server using streaming (SSE/Streamable HTTP) transport."""
    logging.info(f"Starting MCP server with streaming transport on {host}:{port}")
    
    # Import the existing FastMCP server
    from src.common.server import mcp
    
    # Update host and port settings
    mcp.settings.host = host
    mcp.settings.port = port
    
    # FastMCP handles streamable HTTP transport natively
    await mcp.run_streamable_http_async()
