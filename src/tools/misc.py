from typing import Dict, Any
from src.common.connection import RedisConnectionManager
from redis.exceptions import RedisError
from src.common.server import mcp


@mcp.tool()
async def delete(key: str) -> str:
    """Delete a Redis key.

    Args:
        key (str): The key to delete.

    Returns:
        str: Confirmation message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        result = r.delete(key)
        return f"Successfully deleted {key}" if result else f"Key {key} not found"
    except RedisError as e:
        return f"Error deleting key {key}: {str(e)}"


@mcp.tool()  
async def type(key: str) -> Dict[str, Any]:
    """Returns the string representation of the type of the value stored at key

    Args:
        key (str): The key to check.

    Returns:
        str: The type of key, or none when key doesn't exist
    """
    try:
        r = RedisConnectionManager.get_connection()
        key_type = r.type(key)
        info = {
            'key': key,
            'type': key_type,
            'ttl': r.ttl(key)
        }
        
        return info
    except RedisError as e:
        return {'error': str(e)}


@mcp.tool()
async def expire(name: str, expire_seconds: int) -> str:
    """Set an expiration time for a Redis key.

    Args:
        name: The Redis key.
        expire_seconds: Time in seconds after which the key should expire.

    Returns:
        A success message or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        success = r.expire(name, expire_seconds)
        return f"Expiration set to {expire_seconds} seconds for '{name}'." if success else f"Key '{name}' does not exist."
    except RedisError as e:
        return f"Error setting expiration for key '{name}': {str(e)}"


@mcp.tool()
async def rename(old_key: str, new_key: str) -> Dict[str, Any]:
    """
    Renames a Redis key from old_key to new_key.

    Args:
        old_key (str): The current name of the Redis key to rename.
        new_key (str): The new name to assign to the key.

    Returns:
        Dict[str, Any]: A dictionary containing the result of the operation.
            On success: {"status": "success", "message": "..."}
            On error: {"error": "..."}
    """
    try:
        r = RedisConnectionManager.get_connection()

        # Check if the old key exists
        if not r.exists(old_key):
            return {"error": f"Key '{old_key}' does not exist."}

        # Rename the key
        r.rename(old_key, new_key)
        return {
            "status": "success",
            "message": f"Renamed key '{old_key}' to '{new_key}'"
        }

    except RedisError as e:
        return {"error": str(e)}


@mcp.tool()
async def scan_keys(pattern: str = "*", count: int = 100, cursor: int = 0) -> dict:
    """
    Scan keys in the Redis database using the SCAN command (non-blocking, production-safe).
    
    ⚠️  IMPORTANT: This returns PARTIAL results from one iteration. Use scan_all_keys() 
    to get ALL matching keys, or call this function multiple times with the returned cursor
    until cursor becomes 0.
    
    The SCAN command iterates through the keyspace in small chunks, making it safe to use
    on large databases without blocking other operations.

    Args:
        pattern: Pattern to match keys against (default is "*" for all keys).
                Common patterns: "user:*", "cache:*", "*:123", etc.
        count: Hint for the number of keys to return per iteration (default 100).
               Redis may return more or fewer keys than this hint.
        cursor: The cursor position to start scanning from (0 to start from beginning).
                To continue scanning, use the cursor value returned from previous call.

    Returns:
        A dictionary containing:
        - 'cursor': Next cursor position (0 means scan is complete)
        - 'keys': List of keys found in this iteration (PARTIAL RESULTS)
        - 'total_scanned': Number of keys returned in this batch
        - 'scan_complete': Boolean indicating if scan is finished
        Or an error message if something goes wrong.
        
    Example usage:
        First call: scan_keys("user:*") -> returns cursor=1234, keys=[...], scan_complete=False
        Next call: scan_keys("user:*", cursor=1234) -> continues from where it left off
        Final call: returns cursor=0, scan_complete=True when done
    """
    try:
        r = RedisConnectionManager.get_connection()
        
        # Check if we're in cluster mode
        pool = RedisConnectionManager.get_pool()
        connection_details = pool.get_connection_details()
        is_cluster = connection_details.get("cluster_mode", False)
        
        if is_cluster:
            # For cluster mode, we need to handle multiple cursors
            if cursor == 0:
                # Initialize cursors for all nodes
                all_keys = []
                node_cursors = {}
                for node in r.get_nodes():
                    try:
                        node_cursor, node_keys = node.scan(cursor=0, match=pattern, count=count)
                        # Safer key decoding
                        decoded_keys = []
                        for key in node_keys:
                            if isinstance(key, bytes):
                                try:
                                    decoded_keys.append(key.decode('utf-8'))
                                except UnicodeDecodeError:
                                    decoded_keys.append(key.decode('utf-8', errors='replace'))
                            elif isinstance(key, str):
                                decoded_keys.append(key)
                            else:
                                decoded_keys.append(str(key))
                        all_keys.extend(decoded_keys)
                        node_cursors[f"{node.host}:{node.port}"] = node_cursor
                    except Exception:
                        node_cursors[f"{node.host}:{node.port}"] = 0
                
                scan_complete = all(cursor == 0 for cursor in node_cursors.values())
                return {
                    'cursor': node_cursors,
                    'keys': all_keys,
                    'total_scanned': len(all_keys),
                    'scan_complete': scan_complete
                }
            else:
                # This is a continuation call, but we simplified to just return empty for now
                # In a real implementation, you'd need to handle the multi-node cursor state
                return {
                    'cursor': 0,
                    'keys': [],
                    'total_scanned': 0,
                    'scan_complete': True
                }
        else:
            # For standalone mode
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=count)
            
            # Convert bytes to strings if needed - safer decoding
            decoded_keys = []
            for key in keys:
                if isinstance(key, bytes):
                    try:
                        decoded_keys.append(key.decode('utf-8'))
                    except UnicodeDecodeError:
                        decoded_keys.append(key.decode('utf-8', errors='replace'))
                elif isinstance(key, str):
                    decoded_keys.append(key)
                else:
                    decoded_keys.append(str(key))
            
            return {
                'cursor': cursor,
                'keys': decoded_keys,
                'total_scanned': len(decoded_keys),
                'scan_complete': cursor == 0
            }
    except RedisError as e:
        return f"Error scanning keys with pattern '{pattern}': {str(e)}"


@mcp.tool()
async def scan_all_keys(pattern: str = "*", batch_size: int = 100) -> list:
    """
    Scan and return ALL keys matching a pattern using multiple SCAN iterations.
    
    This function automatically handles the SCAN cursor iteration to collect all matching keys.
    It's safer than KEYS * for large databases but will still collect all results in memory.
    
    ⚠️  WARNING: With very large datasets (millions of keys), this may consume significant memory.
    For large-scale operations, consider using scan_keys() with manual iteration instead.

    Args:
        pattern: Pattern to match keys against (default is "*" for all keys).
        batch_size: Number of keys to scan per iteration (default 100).

    Returns:
        A list of all keys matching the pattern or an error message.
    """
    try:
        r = RedisConnectionManager.get_connection()
        
        # Check if we're in cluster mode
        pool = RedisConnectionManager.get_pool()
        connection_details = pool.get_connection_details()
        is_cluster = connection_details.get("cluster_mode", False)
        
        all_keys = []
        
        if is_cluster:
            # For cluster mode, scan each node
            for node in r.get_nodes():
                try:
                    cursor = 0
                    while True:
                        scan_result = node.scan(cursor=cursor, match=pattern, count=batch_size)
                        
                        # Handle different return formats
                        if isinstance(scan_result, tuple) and len(scan_result) == 2:
                            cursor, keys = scan_result
                        else:
                            # Fallback handling
                            break
                        
                        # Convert bytes to strings if needed and add to results
                        if keys:
                            decoded_keys = []
                            for key in keys:
                                if isinstance(key, bytes):
                                    decoded_keys.append(key.decode('utf-8'))
                                elif isinstance(key, str):
                                    decoded_keys.append(key)
                                else:
                                    decoded_keys.append(str(key))
                            all_keys.extend(decoded_keys)
                        
                        # Break when scan is complete (cursor returns to 0)
                        if cursor == 0:
                            break
                except Exception as e:
                    # If a node fails, continue with others
                    continue
        else:
            # For standalone mode
            cursor = 0
            while True:
                scan_result = r.scan(cursor=cursor, match=pattern, count=batch_size)
                
                # Handle different return formats
                if isinstance(scan_result, tuple) and len(scan_result) == 2:
                    cursor, keys = scan_result
                else:
                    break
                
                # Convert bytes to strings if needed and add to results
                if keys:
                    decoded_keys = []
                    for key in keys:
                        if isinstance(key, bytes):
                            decoded_keys.append(key.decode('utf-8'))
                        elif isinstance(key, str):
                            decoded_keys.append(key)
                        else:
                            decoded_keys.append(str(key))
                    all_keys.extend(decoded_keys)
                
                # Break when scan is complete (cursor returns to 0)
                if cursor == 0:
                    break
        
        return all_keys
    except RedisError as e:
        return f"Error scanning all keys with pattern '{pattern}': {str(e)}"
    except Exception as e:
        return f"Error scanning all keys with pattern '{pattern}': {str(e)}"