import sys
import urllib.parse
from typing import Dict, Optional, Type, Union
from src.version import __version__
import redis
from redis import Redis
from redis.cluster import RedisCluster
from src.common.config import REDIS_CFG


class RedisConnectionPool:
    """Manages multiple Redis connections identified by host identifier."""
    
    def __init__(self):
        self._connections: Dict[str, Redis] = {}
        self._default_host: Optional[str] = None
        
    def _create_connection_params(self, config: dict, decode_responses: bool = True) -> dict:
        """Create connection parameters from config dictionary."""
        base_params = {
            "decode_responses": decode_responses,
            "lib_name": f"redis-py(mcp-server_v{__version__})",
        }
        
        # Add all config parameters, filtering out None values
        for key, value in config.items():
            if value is not None and key != "cluster_mode":
                base_params[key] = value
                
        # Set connection limits based on cluster mode
        if config.get("cluster_mode", False):
            base_params["max_connections_per_node"] = 10
        else:
            base_params["max_connections"] = 10
            
        return base_params
    
    def _get_redis_class(self, cluster_mode: bool) -> Type[Union[Redis, RedisCluster]]:
        """Get the appropriate Redis class based on cluster mode."""
        return redis.cluster.RedisCluster if cluster_mode else redis.Redis
    
    def add_connection(self, host_id: str, config: dict, decode_responses: bool = True) -> str:
        """Add a new Redis connection to the pool."""
        try:
            connection_params = self._create_connection_params(config, decode_responses)
            redis_class = self._get_redis_class(config.get("cluster_mode", False))
            
            # Create the connection
            connection = redis_class(**connection_params)
            
            # Test the connection
            connection.ping()
            
            # Store the connection
            self._connections[host_id] = connection
            
            # Set as default if it's the first connection
            if self._default_host is None:
                self._default_host = host_id
                
            return f"Successfully connected to Redis at {host_id}"
            
        except redis.exceptions.ConnectionError:
            raise Exception(f"Failed to connect to Redis server at {host_id}")
        except redis.exceptions.AuthenticationError:
            raise Exception(f"Authentication failed for Redis server at {host_id}")
        except redis.exceptions.TimeoutError:
            raise Exception(f"Connection timed out for Redis server at {host_id}")
        except redis.exceptions.ResponseError as e:
            raise Exception(f"Response error for Redis server at {host_id}: {e}")
        except redis.exceptions.RedisError as e:
            raise Exception(f"Redis error for server at {host_id}: {e}")
        except redis.exceptions.ClusterError as e:
            raise Exception(f"Redis Cluster error for server at {host_id}: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error connecting to Redis server at {host_id}: {e}")
    
    def get_connection(self, host_id: Optional[str] = None) -> Redis:
        """Get a Redis connection by host identifier."""
        if host_id is None:
            host_id = self._default_host
            
        if host_id is None:
            raise Exception("No Redis connections available. Use the 'connect' tool to establish a connection first.")
            
        if host_id not in self._connections:
            raise Exception(f"No connection found for host '{host_id}'. Available hosts: {list(self._connections.keys())}")
            
        return self._connections[host_id]
    
    def list_connections(self) -> Dict[str, dict]:
        """List all active connections with their details."""
        result = {}
        for host_id, conn in self._connections.items():
            try:
                info = conn.info("server")
                result[host_id] = {
                    "status": "connected",
                    "redis_version": info.get("redis_version", "unknown"),
                    "host": getattr(conn, 'host', 'unknown'),
                    "port": getattr(conn, 'port', 'unknown'),
                    "db": getattr(conn, 'db', 'unknown'),
                    "is_default": host_id == self._default_host
                }
            except Exception as e:
                result[host_id] = {
                    "status": f"error: {e}",
                    "is_default": host_id == self._default_host
                }
        return result
    
    def remove_connection(self, host_id: str) -> str:
        """Remove a connection from the pool."""
        if host_id not in self._connections:
            return f"No connection found for host '{host_id}'"
            
        try:
            self._connections[host_id].close()
        except:
            pass  # Ignore close errors
            
        del self._connections[host_id]
        
        # Update default if needed
        if self._default_host == host_id:
            self._default_host = next(iter(self._connections.keys())) if self._connections else None
            
        return f"Connection to '{host_id}' removed successfully"


# Global connection pool instance
_connection_pool = RedisConnectionPool()


class RedisConnectionManager:
    """Legacy compatibility wrapper for the new connection pool."""
    
    @classmethod
    def get_connection(cls, host_id: Optional[str] = None, decode_responses=True) -> Redis:
        """Get a connection for the specified host or the default connection for backward compatibility."""
        # Initialize default connection if none exists and no specific host_id requested
        if not _connection_pool._connections and host_id is None:
            default_host_id = f"{REDIS_CFG['host']}:{REDIS_CFG['port']}"
            _connection_pool.add_connection(default_host_id, REDIS_CFG, decode_responses)
            
        return _connection_pool.get_connection(host_id)
    
    @classmethod
    def get_pool(cls) -> RedisConnectionPool:
        """Get the connection pool instance."""
        return _connection_pool
