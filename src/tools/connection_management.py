from typing import Optional, Dict, Any
from src.common.connection import RedisConnectionManager
from src.common.config import parse_redis_uri
from src.common.server import mcp
import urllib.parse


@mcp.tool()
async def connect(
    url: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl: Optional[bool] = None,
    ssl_ca_path: Optional[str] = None,
    ssl_keyfile: Optional[str] = None,
    ssl_certfile: Optional[str] = None,
    ssl_cert_reqs: Optional[str] = None,
    ssl_ca_certs: Optional[str] = None,
    cluster_mode: Optional[bool] = None,
    host_id: Optional[str] = None
) -> str:
    """Connect to a Redis server and add it to the connection pool.
    
    Args:
        url: Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)
        host: Redis host (default: 127.0.0.1)
        port: Redis port (default: 6379)
        db: Redis database number (default: 0)
        username: Redis username
        password: Redis password
        ssl: Use SSL connection
        ssl_ca_path: Path to CA certificate file
        ssl_keyfile: Path to SSL key file
        ssl_certfile: Path to SSL certificate file
        ssl_cert_reqs: SSL certificate requirements (default: required)
        ssl_ca_certs: Path to CA certificates file
        cluster_mode: Enable Redis cluster mode
        host_id: Custom identifier for this connection (auto-generated if not provided)
    
    Returns:
        Success message with connection details or error message.
    """
    try:
        pool = RedisConnectionManager.get_pool()
        
        # Parse configuration from URL or individual parameters
        if url:
            config = parse_redis_uri(url)
            parsed_url = urllib.parse.urlparse(url)
            # Generate host_id from URL if not provided
            if host_id is None:
                host_id = f"{parsed_url.hostname}:{parsed_url.port or 6379}"
        else:
            # Build config from individual parameters
            config = {
                "host": host or "127.0.0.1",
                "port": port or 6379,
                "db": db or 0,
                "username": username,
                "password": password or "",
                "ssl": ssl or False,
                "ssl_ca_path": ssl_ca_path,
                "ssl_keyfile": ssl_keyfile,
                "ssl_certfile": ssl_certfile,
                "ssl_cert_reqs": ssl_cert_reqs or "required",
                "ssl_ca_certs": ssl_ca_certs,
                "cluster_mode": cluster_mode or False
            }
            # Generate host_id from host:port if not provided
            if host_id is None:
                host_id = f"{config['host']}:{config['port']}"
        
        # Override individual parameters if provided (useful when using URL + specific overrides)
        if host is not None:
            config["host"] = host
        if port is not None:
            config["port"] = port
        if db is not None:
            config["db"] = db
        if username is not None:
            config["username"] = username
        if password is not None:
            config["password"] = password
        if ssl is not None:
            config["ssl"] = ssl
        if ssl_ca_path is not None:
            config["ssl_ca_path"] = ssl_ca_path
        if ssl_keyfile is not None:
            config["ssl_keyfile"] = ssl_keyfile
        if ssl_certfile is not None:
            config["ssl_certfile"] = ssl_certfile
        if ssl_cert_reqs is not None:
            config["ssl_cert_reqs"] = ssl_cert_reqs
        if ssl_ca_certs is not None:
            config["ssl_ca_certs"] = ssl_ca_certs
        if cluster_mode is not None:
            config["cluster_mode"] = cluster_mode
        
        # Add connection to pool
        result = pool.add_connection(host_id, config)
        
        return f"{result}. Host identifier: '{host_id}'"
        
    except Exception as e:
        return f"Failed to connect to Redis: {str(e)}"


@mcp.tool()
async def list_connections() -> Dict[str, Any]:
    """List all active Redis connections in the pool.
    
    Returns:
        Dictionary containing details of all active connections.
    """
    try:
        pool = RedisConnectionManager.get_pool()
        connections = pool.list_connections()
        
        if not connections:
            return {"message": "No active connections", "connections": {}}
        
        return {
            "message": f"Found {len(connections)} active connection(s)",
            "connections": connections
        }
        
    except Exception as e:
        return {"error": f"Failed to list connections: {str(e)}"}


@mcp.tool()
async def disconnect(host_id: str) -> str:
    """Disconnect from a Redis server and remove it from the connection pool.
    
    Args:
        host_id: The identifier of the connection to remove
    
    Returns:
        Success message or error message.
    """
    try:
        pool = RedisConnectionManager.get_pool()
        result = pool.remove_connection(host_id)
        return result
        
    except Exception as e:
        return f"Failed to disconnect from {host_id}: {str(e)}"


@mcp.tool()
async def switch_default_connection(host_id: str) -> str:
    """Switch the default connection to a different host.
    
    Args:
        host_id: The identifier of the connection to set as default
    
    Returns:
        Success message or error message.
    """
    try:
        pool = RedisConnectionManager.get_pool()
        
        # Check if connection exists
        if host_id not in pool._connections:
            available = list(pool._connections.keys())
            return f"Connection '{host_id}' not found. Available connections: {available}"
        
        # Set as default
        pool._default_host = host_id
        return f"Default connection switched to '{host_id}'"
        
    except Exception as e:
        return f"Failed to switch default connection: {str(e)}"
