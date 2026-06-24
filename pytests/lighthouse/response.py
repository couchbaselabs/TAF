
"""
Response and Error utilities for UCP API Client
"""
import json
from typing import Dict, Any, Optional, List

class UCPResponse:
    """Helper class to parse UCP API responses."""
    def __init__(self, status: bool, content: str, response: Any):
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
            except json.JSONDecodeError:
                self._json_data = None

    @property
    def json(self) -> Optional[Dict[str, Any]]:
        """Get parsed JSON data."""
        return self._json_data
    @property
    def status_code(self) -> int:
        """Get HTTP status code."""
        return self.response.status_code if self.response else None
    @property
    def headers(self) -> Dict[str, str]:
        """Get response headers."""
        return dict(self.response.headers) if self.response else {}
    @property
    def etag(self) -> Optional[str]:
        """Get ETag header if present."""
        return self.headers.get('ETag') or self.headers.get('etag')
    @property
    def x_request_id(self) -> Optional[str]:
        """Get X-Request-Id header if present."""
        return self.headers.get('X-Request-Id') or self.headers.get('x-request-id')
    def is_success(self) -> bool:
        """Check if response is successful."""
        return self.status

    def is_error(self) -> bool:
        """Check if response is an error."""
        return not self.status

    def get_error_message(self) -> Optional[str]:
        """Extract error message from response."""
        if self._json_data and isinstance(self._json_data, dict):
            if 'detail' in self._json_data:
                return self._json_data['detail']
            if 'error' in self._json_data:
                return self._json_data['error']
            if 'message' in self._json_data:
                return self._json_data['message']
        return None

    def get_error_code(self) -> Optional[str]:
        """Extract error code from response."""
        if self._json_data and isinstance(self._json_data, dict):
            return self._json_data.get('code')
        return None

    def get_validation_errors(self) -> Optional[List[Dict[str, Any]]]:
        """Extract validation errors array if present."""
        if self._json_data and isinstance(self._json_data, dict):
            return self._json_data.get('errors')
        return None

    def __repr__(self) -> str:
        """String representation."""
        return (f"UCPResponse(status={self.status}, status_code={self.status_code}, "
                f"etag={self.etag}, x_request_id={self.x_request_id})")

class UCPError(Exception):
    """Base exception for UCP API errors."""
    def __init__(self, message: str, status_code: Optional[int] = None,
                 error_code: Optional[str] = None):
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
        super().__init__(message)

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

def raise_for_status(response: UCPResponse) -> None:
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
    error_msg = response.get_error_message() or f"HTTP {status_code}"
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
