import aiohttp
import socket
import logging

log = logging.getLogger(__name__)

def get_dns_resolver() -> aiohttp.ThreadedResolver:
    """
    Returns a ThreadedResolver. This avoids the need for aiodns.
    """
    return aiohttp.ThreadedResolver()

def get_connector(limit: int = 100, limit_per_host: int = 20, force_ipv4: bool = True) -> aiohttp.TCPConnector:
    """
    Returns a TCPConnector with custom DNS resolver and optimized pooling.
    
    Args:
        limit: Total connection pool size
        limit_per_host: Max connections per host
        force_ipv4: If True, forces IPv4 (AF_INET) to avoid IPv6 timeouts
    """
    family = socket.AF_INET if force_ipv4 else 0
    resolver = get_dns_resolver()
    
    log.info(f"Creating robust TCPConnector (IPv4={force_ipv4}, DNS=8.8.8.8/1.1.1.1)")
    
    return aiohttp.TCPConnector(
        limit=limit,
        limit_per_host=limit_per_host,
        resolver=resolver,
        family=family,
        ttl_dns_cache=300  # Cache DNS results for 5 minutes
    )
