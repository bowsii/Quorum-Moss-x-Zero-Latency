"""
quorum/api/middleware/security_headers.py
------------------------------------------
Security headers middleware.

Attaches a standard set of defensive HTTP response headers to every response
served by the Quorum API. These headers protect against common web
vulnerabilities (clickjacking, MIME sniffing, XSS, etc.) with no runtime
overhead beyond the string writes.

Headers applied
---------------
- X-Content-Type-Options: nosniff
- X-Frame-Options: DENY
- X-XSS-Protection: 1; mode=block
- Content-Security-Policy: default-src 'self'
- Strict-Transport-Security: max-age=31536000; includeSubDomains
- Referrer-Policy: no-referrer
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Security header definitions - centralised so they are easy to audit/update.
_SECURITY_HEADERS: dict[str, str] = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "1; mode=block",
    "Content-Security-Policy": "default-src 'self'",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "Referrer-Policy": "no-referrer",
}


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Append defensive security headers to every outgoing HTTP response.

    Registered as ASGI middleware on the FastAPI application so that all
    routes - including error responses - receive the headers automatically.

    Usage
    -----
    .. code-block:: python

        from api.middleware.security_headers import SecurityHeadersMiddleware

        app.add_middleware(SecurityHeadersMiddleware)
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        """Forward the request then inject security headers into the response.

        Parameters
        ----------
        request:
            The incoming Starlette request (passed through unchanged).
        call_next:
            Callable that forwards the request to the next middleware or route
            handler and returns the raw response.

        Returns
        -------
        Response
            The downstream response with security headers added.
        """
        response: Response = await call_next(request)
        for header_name, header_value in _SECURITY_HEADERS.items():
            response.headers[header_name] = header_value
        return response
