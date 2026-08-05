
"""
Response and Error utilities for UCP API Client
NOTE: must stay Jython/Python-2 compatible -- no type annotations,
no f-strings, no json.JSONDecodeError (Py3-only).
"""
import json

class UCPResponse(object):
    """Helper class to parse UCP API responses."""
    def __init__(self, status, content, response):
        """
        Initialize response wrapper.
        Args:
            status: Boolean status from _http_request
            content: Raw response content string
            response: requests.Response object
        """
        self.status = status
        self.content = content
        self.response = response
        self._json_data = None
        self._parse_json()

    def _parse_json(self):
        """Parse JSON content if possible."""
        if self.content:
            try:
                self._json_data = json.loads(self.content)
            except ValueError:
                self._json_data = None

    @property
    def json(self):
        """Get parsed JSON data."""
        return self._json_data

    @property
    def items(self):
        """
        List of records from a list-endpoint body. Handles a bare list or
        an envelope keyed by items/data/users/clusters/results.
        """
        data = self._json_data
        if data is None:
            return []
        if isinstance(data, list):
            return data
        for key in ('items', 'data', 'users', 'clusters', 'results'):
            value = data.get(key)
            if isinstance(value, list):
                return value
        return []

    @property
    def total(self):
        """
        Reported total count from a list-endpoint body, or None if the body
        carries no total field.
        """
        data = self._json_data
        if isinstance(data, dict):
            for key in ('total', 'totalCount', 'totalItems', 'count'):
                if key in data:
                    return data[key]
        return None

    @property
    def status_code(self):
        """Get HTTP status code."""
        return self.response.status_code if self.response is not None else None

    @property
    def headers(self):
        """Get response headers."""
        return dict(self.response.headers) if self.response is not None else {}

    def get_header(self, name):
        """
        Case-insensitive lookup of a single response header value.
        HTTP/2 delivers header names lower-cased while HTTP/1.1 preserves the
        sender's casing, so every header read must be case-insensitive.
        Args:
            name: header name to look up, in any casing
        Returns:
            Header value string, or None if the header is absent.
        """
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return None

    @property
    def etag(self):
        """Get ETag header if present."""
        return self.get_header('ETag')

    @property
    def set_cookie(self):
        """
        Raw Set-Cookie header value exactly as sent by the portal, or None.

        Distinct from the client's stored session cookie: the client keeps only
        the leading "name=value" pair (see UnifiedControlPlaneClient.
        session_login) and discards everything after the first ';', so the
        security attributes (HttpOnly, Secure, SameSite) are only visible here.
        Pair with ucp_helper_methods.parse_cookie_attributes to inspect them.
        """
        return self.get_header('Set-Cookie')

    @property
    def x_request_id(self):
        """Get X-Request-Id header if present."""
        return (self.headers.get('X-Request-Id')
                or self.headers.get('x-request-id'))

    def is_success(self):
        """Check if response is successful."""
        return self.status

    def is_error(self):
        """Check if response is an error."""
        return not self.status

    def get_error_message(self):
        """Extract error message from response."""
        if self._json_data and isinstance(self._json_data, dict):
            if 'detail' in self._json_data:
                return self._json_data['detail']
            if 'error' in self._json_data:
                return self._json_data['error']
            if 'message' in self._json_data:
                return self._json_data['message']
        return None

    def get_error_code(self):
        """Extract error code from response."""
        if self._json_data and isinstance(self._json_data, dict):
            return self._json_data.get('code')
        return None

    def get_validation_errors(self):
        """Extract validation errors array if present."""
        if self._json_data and isinstance(self._json_data, dict):
            return self._json_data.get('errors')
        return None

    def __repr__(self):
        """String representation."""
        return ("UCPResponse(status=%s, status_code=%s, "
                "etag=%s, x_request_id=%s)"
                % (self.status, self.status_code,
                   self.etag, self.x_request_id))

class UCPError(Exception):
    """Base exception for UCP API errors."""
    def __init__(self, message, status_code=None, error_code=None):
        """
        Initialize UCP error.
        Args:
            message: Error message
            status_code: HTTP status code
            error_code: UCP error code
        """
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        super(UCPError, self).__init__(message)

class ValidationError(UCPError):
    """Validation error (422)."""
    pass
class UnauthorizedError(UCPError):
    """Unauthorized error (401)."""
    pass
class ForbiddenError(UCPError):
    """Forbidden error (403)."""
    pass
class NotFoundError(UCPError):
    """Not found error (404)."""
    pass
class ConflictError(UCPError):
    """Conflict error (409)."""
    pass
class PreconditionFailedError(UCPError):
    """Precondition failed error (412)."""
    pass
class InternalError(UCPError):
    """Internal error (500)."""
    pass

def raise_for_status(response):
    """
    Raise appropriate exception based on response status.
    Args:
        response: UCPResponse object
    Raises:
        Appropriate UCPError subclass
    """
    if response.is_success():
        return
    status_code = response.status_code
    error_code = response.get_error_code()
    error_msg = response.get_error_message() or "HTTP %s" % status_code
    if status_code == 422:
        raise ValidationError(error_msg, status_code, error_code)
    elif status_code == 401:
        raise UnauthorizedError(error_msg, status_code, error_code)
    elif status_code == 403:
        raise ForbiddenError(error_msg, status_code, error_code)
    elif status_code == 404:
        raise NotFoundError(error_msg, status_code, error_code)
    elif status_code == 409:
        raise ConflictError(error_msg, status_code, error_code)
    elif status_code == 412:
        raise PreconditionFailedError(error_msg, status_code, error_code)
    elif status_code == 500:
        raise InternalError(error_msg, status_code, error_code)
    else:
        raise UCPError(error_msg, status_code, error_code)
