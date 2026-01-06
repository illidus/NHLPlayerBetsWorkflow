import os
from typing import Optional, Tuple

DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_READ_TIMEOUT = 30
MAX_RETRIES = 3


class VendorRequestError(RuntimeError):
    """Raised for vendor request failures so callers can distinguish vendor errors."""


def get_timeout_tuple(read_timeout: Optional[int] = None) -> Tuple[int, int]:
    if read_timeout is None:
        read_timeout = DEFAULT_READ_TIMEOUT
    return (DEFAULT_CONNECT_TIMEOUT, int(read_timeout))


def should_force_vendor_failure(vendor_name: str) -> bool:
    vendor_name = vendor_name.upper()
    force_all = os.environ.get("FORCE_VENDOR_FAILURE", "").upper()
    if force_all == vendor_name or force_all == "ALL":
        return True
    return os.environ.get(f"FORCE_{vendor_name}_FAILURE", "0") == "1"
