import httpx
import asyncio


class HTTPClientManager:
    """
    Manages a shared httpx.AsyncClient instance with concurrency control.
    """
    def __init__(self, timeout: float = 30.0):
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    async def initialize(self):
        if not self._client:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
            )

    async def cleanup(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def post(self, url: str, **kwargs) -> httpx.Response:
        async with self._lock:
            if not self._client:
                raise RuntimeError("HTTP client not initialized")
            return await self._client.post(url, **kwargs)
