"""
quorum/api/middleware/rate_limit.py
-------------------------------------
Sliding-window in-process rate limiter middleware.

Limits each unique client IP to a configurable number of requests per minute.
Excess requests receive HTTP 429 Too Many Requests with a Retry-After header
indicating how many seconds the client must wait before retrying.

Implementation
--------------
Uses a per-IP collections.deque of monotonic timestamps. On each request,
timestamps older than the sliding window (60 s) are evicted; if the remaining
count still exceeds the limit the request is rejected immediately. No external
state store is required, making this suitable for single-process deployments.

For multi-process / distributed deployments, replace the in-memory store with
a Redis-backed counter (e.g. using redis.asyncio).
"""

import asyncio
import math
import time
from collections import defaultdict, deque
from typing import DefaultDict, Deque

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window rate limiter: 100 requests per minute per IP by default.

    Parameters
    ----------
    app:
        The ASGI application to wrap.
    max_requests:
        Maximum number of requests allowed within the window period.
    window_seconds:
        Length of the sliding window in seconds (default 60).
    """

    def __init__(
        self,
        app,
        max_requests: int = 100,
        window_seconds: int = 60,
    ) -> None:
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        # ip -> deque of request timestamps (monotonic seconds)
        self._counters: DefaultDict[str, Deque[float]] = defaultdict(deque)
        # Lock per IP to prevent race conditions under concurrent async access.
        self._locks: DefaultDict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    # ------------------------------------------------------------------
    # ASGI dispatch
    # ------------------------------------------------------------------

    async def dispatch(self, request: Request, call_next) -> Response:
        """Intercept every request, enforce the rate limit, then call next.

        Parameters
        ----------
        request:
            The incoming Starlette request.
        call_next:
            Callable that forwards the request to the next middleware or route.

        Returns
        -------
        Response
            HTTP 429 if the limit is exceeded; otherwise the downstream response.
        """
        client_ip = self._resolve_ip(request)
        now = time.monotonic()

        async with self._locks[client_ip]:
            window = self._counters[client_ip]

            # Evict timestamps that have fallen outside the sliding window.
            cutoff = now - self.window_seconds
            while window and window[0] <= cutoff:
                window.popleft()

            if len(window) >= self.max_requests:
                # Calculate how long until the oldest request ages out.
                retry_after = math.ceil(self.window_seconds - (now - window[0]))
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": "Rate limit exceeded. Please slow down.",
                        "retry_after_seconds": retry_after,
                    },
                    headers={"Retry-After": str(retry_after)},
                )

            window.append(now)

        return await call_next(request)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_ip(request: Request) -> str:
        """Extract the real client IP, respecting common proxy headers.

        Priority: X-Forwarded-For (first hop) > X-Real-IP > direct client host.

        Parameters
        ----------
        request:
            The incoming Starlette request.

        Returns
        -------
        str
            The resolved client IP address string.
        """
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take only the first (leftmost) address - the original client.
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()

        if request.client:
            return request.client.host

        return "unknown"
