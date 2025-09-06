import os
import json
import hashlib
import httpx
from pathlib import Path
from typing import Optional, Dict, Any, Union
from datetime import datetime
from ratelimit import limits, sleep_and_retry
import logging
from . import debug
from .environment import get_data_dir

logger = logging.getLogger(__name__)

_client = None
_client_config = {
    'timeout': int(os.environ.get('HTTP_TIMEOUT', '30')),
    'cache_enabled': os.environ.get('ENABLE_HTTP_CACHE', '').lower() == 'true',
    'rate_limit_calls': int(os.environ.get('HTTP_RATE_LIMIT_CALLS', '60')),
    'rate_limit_period': int(os.environ.get('HTTP_RATE_LIMIT_PERIOD', '60')),
    'cache_dir': Path(os.environ.get('HTTP_CACHE_DIR', f'{get_data_dir()}/http_cache')),
    'headers': {'User-Agent': os.environ.get('HTTP_USER_AGENT', 'DataIntegrations/1.0')}
}

class CacheManager:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        self.requests_log = []
        self.run_id = os.environ.get('RUN_ID', 'unknown')
        self.connector = os.environ.get('CONNECTOR_NAME', 'unknown')
        
    def _cache_key(self, method: str, url: str, params: Optional[Dict] = None) -> str:
        key_parts = [method, url]
        if params:
            key_parts.append(json.dumps(sorted(params.items())))
        return hashlib.md5("".join(key_parts).encode()).hexdigest()
    
    def get(self, method: str, url: str, **kwargs) -> Optional[httpx.Response]:
        key = self._cache_key(method, url, kwargs.get("params"))
        metadata_file = self.cache_dir / f"{key}.meta.json"
        content_file = self.cache_dir / f"{key}.bin"
        
        if metadata_file.exists() and content_file.exists():
            # Load metadata
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            # Load raw content bytes
            with open(content_file, 'rb') as f:
                content = f.read()
            
            # Get headers and remove encoding-related ones since content is raw
            headers = metadata.get("headers", {})
            headers.pop("content-encoding", None)
            headers.pop("transfer-encoding", None)
                
            # Log cached request
            debug.log_http_request(
                method=method,
                url=url,
                params=kwargs.get("params"),
                headers=kwargs.get("headers"),
                response_status=metadata["status_code"],
                response_size=len(content),
                cached=True,
                cache_key=key
            )
            
            return httpx.Response(
                status_code=metadata["status_code"],
                headers=headers,
                content=content,
                request=httpx.Request(method, url)
            )
        
        return None
    
    def save(self, method: str, url: str, response: httpx.Response, **kwargs):
        key = self._cache_key(method, url, kwargs.get("params"))
        metadata_file = self.cache_dir / f"{key}.meta.json"
        content_file = self.cache_dir / f"{key}.bin"
        
        # Save raw content bytes
        with open(content_file, 'wb') as f:
            f.write(response.content)
        
        # Save metadata
        metadata = {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "url": url,
            "method": method,
            "cached_at": datetime.now().isoformat()
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        # Log new request
        elapsed = response.elapsed.total_seconds() if hasattr(response, 'elapsed') else None
        debug.log_http_request(
            method=method,
            url=url,
            params=kwargs.get("params"),
            headers=kwargs.get("headers"),
            response_status=response.status_code,
            response_headers=dict(response.headers),
            response_size=len(response.content) if response.content else 0,
            duration_ms=int(elapsed * 1000) if elapsed else None,
            cached=False,
            cache_key=key
        )

class CachedClient:
    def __init__(self, client: httpx.Client, cache_manager: CacheManager):
        self.client = client
        self.cache = cache_manager
        
    def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        start_time = datetime.now()
        error = None
        response = None
        cached = False
        
        try:
            if _client_config['cache_enabled']:
                cached_response = self.cache.get(method, url, **kwargs)
                if cached_response:
                    cached = True
                    return cached_response
            
            response = self.client.request(method, url, **kwargs)
            
            if _client_config['cache_enabled'] and response.status_code < 400:
                self.cache.save(method, url, response, **kwargs)
                
            return response
        except Exception as e:
            error = str(e)
            raise
        finally:
            # Log non-cached requests
            if not _client_config['cache_enabled'] or not cached:
                elapsed = (datetime.now() - start_time).total_seconds()
                debug.log_http_request(
                    method=method,
                    url=url,
                    params=kwargs.get("params"),
                    headers=kwargs.get("headers"),
                    request_body=kwargs.get("json") or kwargs.get("data"),
                    response_status=response.status_code if response else None,
                    response_headers=dict(response.headers) if response else None,
                    response_size=len(response.content) if response and response.content else None,
                    duration_ms=int(elapsed * 1000),
                    cached=False,
                    error=error
                )
    
    def get(self, url: str, **kwargs) -> httpx.Response:
        return self.request("GET", url, **kwargs)
    
    def post(self, url: str, **kwargs) -> httpx.Response:
        return self.request("POST", url, **kwargs)
    
    def put(self, url: str, **kwargs) -> httpx.Response:
        return self.request("PUT", url, **kwargs)
    
    def delete(self, url: str, **kwargs) -> httpx.Response:
        return self.request("DELETE", url, **kwargs)
    
    def close(self):
        self.client.close()

def _create_base_client() -> httpx.Client:
    client = httpx.Client(
        timeout=_client_config['timeout'],
        headers=_client_config['headers'],
        follow_redirects=True
    )
    
    # Store the original request method before reassigning
    original_request = client.request
    
    @sleep_and_retry
    @limits(calls=_client_config['rate_limit_calls'], period=_client_config['rate_limit_period'])
    def _rate_limited_request(method: str, url: str, **kwargs):
        return original_request(method, url, **kwargs)
    
    client.request = _rate_limited_request
    
    return client

def _get_or_create_client(**overrides) -> Union[httpx.Client, CachedClient]:
    global _client
    
    if _client is None:
        config = _client_config.copy()
        config.update(overrides)
        
        base_client = _create_base_client()
        
        if config['cache_enabled']:
            cache_manager = CacheManager(config['cache_dir'])
            _client = CachedClient(base_client, cache_manager)
        else:
            _client = base_client
            
    return _client

def get(url: str, **kwargs) -> httpx.Response:
    client = _get_or_create_client()
    return client.get(url, **kwargs)

def post(url: str, **kwargs) -> httpx.Response:
    client = _get_or_create_client()
    return client.post(url, **kwargs)

def put(url: str, **kwargs) -> httpx.Response:
    client = _get_or_create_client()
    return client.put(url, **kwargs)

def delete(url: str, **kwargs) -> httpx.Response:
    client = _get_or_create_client()
    return client.delete(url, **kwargs)

def get_client(**overrides) -> Union[httpx.Client, CachedClient]:
    return _get_or_create_client(**overrides)

def configure_http(**config):
    global _client_config, _client
    _client_config.update(config)
    if _client:
        _client.close()
        _client = None