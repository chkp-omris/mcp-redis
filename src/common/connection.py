import sys
import urllib.parse
from typing import Dict, Optional, Type, Union, Any
from src.version import __version__
import redis
from redis import Redis
from redis.cluster import RedisCluster, ClusterNode


class RedisConnectionPool:
    """Manages multiple Redis connections identified by host identifier (Singleton)."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisConnectionPool, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._connections: Dict[str, Redis] = {}
            self._configs: Dict[str, dict] = {}  # Store original configurations
            self._default_host: Optional[str] = None
            self._initialized = True
        
    def _create_connection_params(self, config: dict, decode_responses: bool = True) -> dict:
        """Create connection parameters from config dictionary."""
        base_params = {
            "decode_responses": decode_responses,
            "lib_name": f"redis-py(mcp-server_v{__version__})",
        }
        
        cluster_mode = config.get("cluster_mode", False)
        
        if cluster_mode:
            # For cluster mode, we need to use startup_nodes instead of host/port
            host = config.get("host", "127.0.0.1")
            port = config.get("port", 6379)
            startup_nodes = [ClusterNode(host=host, port=port)]
            base_params["startup_nodes"] = startup_nodes
            base_params["max_connections_per_node"] = 10
            
            # Add cluster-specific parameters, excluding host, port, db
            cluster_incompatible_keys = {"host", "port", "db", "cluster_mode"}
            for key, value in config.items():
                if value is not None and key not in cluster_incompatible_keys:
                    base_params[key] = value
        else:
            # For non-cluster mode, add all config parameters except cluster_mode
            for key, value in config.items():
                if value is not None and key != "cluster_mode":
                    base_params[key] = value
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
            
            # Debug logging for cluster mode
            cluster_mode = config.get("cluster_mode", False)
            print(f"DEBUG: Cluster mode: {cluster_mode}", file=sys.stderr)
            print(f"DEBUG: Config: {config}", file=sys.stderr)
            print(f"DEBUG: Connection params: {connection_params}", file=sys.stderr)
            print(f"DEBUG: Redis class: {redis_class}", file=sys.stderr)
            
            connection = redis_class(**connection_params)
            connection.ping()
            
            # Store the connection and its configuration
            self._connections[host_id] = connection
            self._configs[host_id] = config.copy()  # Store original config
            
            # Set as default if it's the first connection
            if self._default_host is None:
                self._default_host = host_id
                
            return f"Successfully connected to Redis at {host_id}"
            
        except redis.exceptions.ConnectionError as e:
            raise Exception(f"Failed to connect to Redis server at {host_id}: {e}")
        except redis.exceptions.AuthenticationError as e:
            raise Exception(f"Authentication failed for Redis server at {host_id}: {e}")
        except redis.exceptions.TimeoutError as e:
            raise Exception(f"Connection timed out for Redis server at {host_id}: {e}")
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
                config = self._configs.get(host_id, {})
                
                result[host_id] = {
                    "status": "connected",
                    "redis_version": info.get("redis_version", "unknown"),
                    "host": config.get("host", getattr(conn, 'host', 'unknown')),
                    "port": config.get("port", getattr(conn, 'port', 'unknown')),
                    "db": config.get("db", getattr(conn, 'db', 'unknown')),
                    "cluster_mode": config.get("cluster_mode", False),
                    "ssl": config.get("ssl", False),
                    "is_default": host_id == self._default_host
                }
            except Exception as e:
                config = self._configs.get(host_id, {})
                result[host_id] = {
                    "status": f"error: {e}",
                    "host": config.get("host", "unknown"),
                    "port": config.get("port", "unknown"),
                    "db": config.get("db", "unknown"),
                    "cluster_mode": config.get("cluster_mode", False),
                    "ssl": config.get("ssl", False),
                    "is_default": host_id == self._default_host
                }
        return result
    
    def get_connection_details(self, host_id: Optional[str] = None) -> Dict[str, Any]:
        """Get details for a specific connection or the default connection."""
        if host_id is None:
            host_id = self._default_host
            
        if host_id is None:
            return {"error": "No Redis connections available"}
            
        if host_id not in self._connections:
            available = list(self._connections.keys())
            return {"error": f"Connection '{host_id}' not found. Available connections: {available}"}
        
        conn = self._connections[host_id]
        config = self._configs.get(host_id, {})
        
        try:
            info = conn.info("server")
            return {
                "host_id": host_id,
                "status": "connected",
                "redis_version": info.get("redis_version", "unknown"),
                "host": config.get("host", getattr(conn, 'host', 'unknown')),
                "port": config.get("port", getattr(conn, 'port', 'unknown')),
                "db": config.get("db", getattr(conn, 'db', 'unknown')),
                "cluster_mode": config.get("cluster_mode", False),
                "ssl": config.get("ssl", False),
                "is_default": host_id == self._default_host
            }
        except Exception as e:
            return {
                "host_id": host_id,
                "status": f"error: {e}",
                "host": config.get("host", "unknown"),
                "port": config.get("port", "unknown"),
                "db": config.get("db", "unknown"),
                "cluster_mode": config.get("cluster_mode", False),
                "ssl": config.get("ssl", False),
                "is_default": host_id == self._default_host
            }
    
    def remove_connection(self, host_id: str) -> str:
        """Remove a connection from the pool."""
        if host_id not in self._connections:
            return f"No connection found for host '{host_id}'"
            
        try:
            self._connections[host_id].close()
        except:
            pass  # Ignore close errors
            
        # Remove both connection and config
        del self._connections[host_id]
        self._configs.pop(host_id, None)  # Remove config, ignore if not found
        
        # Update default if needed
        if self._default_host == host_id:
            self._default_host = next(iter(self._connections.keys())) if self._connections else None
            
        return f"Connection to '{host_id}' removed successfully"
    
    @classmethod
    def get_instance(cls) -> 'RedisConnectionPool':
        """Get the singleton instance."""
        return cls()
    
    @classmethod
    def add_connection_to_pool(cls, host_id: str, config: dict, decode_responses: bool = True) -> str:
        """Class method to add a connection to the singleton pool."""
        return cls.get_instance().add_connection(host_id, config, decode_responses)
    
    @classmethod
    def get_connection_from_pool(cls, host_id: Optional[str] = None) -> Redis:
        """Class method to get a connection from the singleton pool."""
        return cls.get_instance().get_connection(host_id)
    
    @classmethod
    def list_connections_in_pool(cls) -> Dict[str, dict]:
        """Class method to list all connections in the singleton pool."""
        return cls.get_instance().list_connections()
    
    @classmethod
    def remove_connection_from_pool(cls, host_id: str) -> str:
        """Class method to remove a connection from the singleton pool."""
        return cls.get_instance().remove_connection(host_id)
    
    @classmethod
    def get_connection_details_from_pool(cls, host_id: Optional[str] = None) -> Dict[str, Any]:
        """Class method to get connection details from the singleton pool."""
        return cls.get_instance().get_connection_details(host_id)

def get_connection(host_id: Optional[str] = None) -> Redis:
    """Get a Redis connection by host identifier (legacy function)."""
    return RedisConnectionPool.get_connection_from_pool(host_id)

def get_connection_pool() -> RedisConnectionPool:
    """Get the connection pool instance (legacy function)."""
    return RedisConnectionPool.get_instance()


class RedisConnectionManager:
    """Compatibility wrapper for the connection pool."""
    
    @classmethod
    def get_connection(cls, host_id: Optional[str] = None, decode_responses=True) -> Redis:
        """Get a connection for the specified host or the default connection."""
        pool = RedisConnectionPool.get_instance()
        # Initialize default connection if none exists and no specific host_id requested
        if not pool._connections and host_id is None:
            # Create default configuration from environment variables
            from src.common.config import RedisConfig
            default_config = RedisConfig()
            default_host_id = f"{default_config['host']}:{default_config['port']}"
            pool.add_connection(default_host_id, default_config.config, decode_responses)
            
        return pool.get_connection(host_id)
    
    @classmethod
    def get_pool(cls) -> RedisConnectionPool:
        """Get the connection pool instance."""
        return RedisConnectionPool.get_instance()
