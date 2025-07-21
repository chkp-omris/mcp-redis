import sys
import click
import asyncio

from src.common.config import parse_redis_uri, set_redis_config_from_cli
from src.common.stdio_server import serve_stdio
from src.common.streaming_server import serve_streaming
import src.tools.server_management
import src.tools.misc
import src.tools.redis_query_engine
import src.tools.hash
import src.tools.list
import src.tools.string
import src.tools.json
import src.tools.sorted_set
import src.tools.set
import src.tools.stream
import src.tools.pub_sub


@click.command()
@click.option('--transport', default='stdio', type=click.Choice(['stdio', 'streamable-http']), 
              help='Transport method (stdio or streamable-http)')
@click.option('--http-host', default='127.0.0.1', help='HTTP server host (for streamable-http transport)')
@click.option('--http-port', default=8000, type=int, help='HTTP server port (for streamable-http transport)')
@click.option('--url', help='Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)')
@click.option('--host', default='127.0.0.1', help='Redis host')
@click.option('--port', default=6379, type=int, help='Redis port')
@click.option('--db', default=0, type=int, help='Redis database number')
@click.option('--username', help='Redis username')
@click.option('--password', help='Redis password')
@click.option('--ssl', is_flag=True, help='Use SSL connection')
@click.option('--ssl-ca-path', help='Path to CA certificate file')
@click.option('--ssl-keyfile', help='Path to SSL key file')
@click.option('--ssl-certfile', help='Path to SSL certificate file')
@click.option('--ssl-cert-reqs', default='required', help='SSL certificate requirements')
@click.option('--ssl-ca-certs', help='Path to CA certificates file')
@click.option('--cluster-mode', is_flag=True, help='Enable Redis cluster mode')
def cli(transport, http_host, http_port, url, host, port, db, username, password,
        ssl, ssl_ca_path, ssl_keyfile, ssl_certfile,
        ssl_cert_reqs, ssl_ca_certs, cluster_mode):
    """Redis MCP Server - Model Context Protocol server for Redis."""
    
    print("Starting the Redis MCP Server", file=sys.stderr)

    # Handle Redis URI if provided
    if url:
        try:
            uri_config = parse_redis_uri(url)
            set_redis_config_from_cli(uri_config)
        except ValueError as e:
            click.echo(f"Error parsing Redis URI: {e}", err=True)
            sys.exit(1)
    else:
        # Set individual Redis parameters
        config = {
            'host': host,
            'port': port,
            'db': db,
            'ssl': ssl,
            'cluster_mode': cluster_mode
        }

        if username:
            config['username'] = username
        if password:
            config['password'] = password
        if ssl_ca_path:
            config['ssl_ca_path'] = ssl_ca_path
        if ssl_keyfile:
            config['ssl_keyfile'] = ssl_keyfile
        if ssl_certfile:
            config['ssl_certfile'] = ssl_certfile
        if ssl_cert_reqs:
            config['ssl_cert_reqs'] = ssl_cert_reqs
        if ssl_ca_certs:
            config['ssl_ca_certs'] = ssl_ca_certs

        set_redis_config_from_cli(config)

    # Start the appropriate server
    if transport == "streamable-http":
        print(f"Starting streamable HTTP server on {http_host}:{http_port}", file=sys.stderr)
        asyncio.run(serve_streaming(host=http_host, port=http_port))
    else:
        asyncio.run(serve_stdio())


def main():
    """Legacy main function for backward compatibility."""
    cli()


if __name__ == "__main__":
    main()
