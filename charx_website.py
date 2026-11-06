#######################################
# CHARX Website (WBM) helpers
#
# Firmware-aware login + log download for
# the DOWNLOAD_LOGS command. Library only —
# not started as a process by update.py.
#
# @ 2024 - 2026 EWE s.r.o.
# WWW: mobility.ewe.cz
#######################################

import logging
import re

from typing import Any, Dict, Optional, Tuple

from utils import send_request


# Phoenix REST returns e.g. "V1.7.0" or "V1.9.1-release+488139...."
_FW_VERSION_RE = re.compile(r"^[Vv]?(\d+)\.(\d+)\.(\d+)")


def _parse_firmware_tuple(version_str: Optional[str]) -> Optional[Tuple[int, int, int]]:
    """
    Parse a leading N.N.N from a firmware version string.

    Accepts an optional V/v prefix and ignores any suffix after patch
    (e.g. "V1.9.1-release+...").

    Returns:
        (major, minor, patch) or None if missing/unparseable.
    """

    if not version_str or not isinstance(version_str, str):
        return None

    match = _FW_VERSION_RE.match(version_str.strip())
    if not match:
        return None

    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def firmware_at_least(
    version_str: Optional[str],
    major: int,
    minor: int,
    patch: int,
) -> bool:
    """
    Return True if version_str is at least major.minor.patch.

    Missing or unparseable versions are treated as pre-1.9 (safer default
    for older devices) and a warning is logged.
    """

    parsed = _parse_firmware_tuple(version_str)
    if parsed is None:
        logging.warning(
            "Unparseable or missing firmware version %r; treating as pre-1.9.0",
            version_str,
        )
        return False

    return parsed >= (major, minor, patch)


def wbm_base_url(firmware_version: Optional[str], host: str) -> Tuple[str, bool]:
    """
    Resolve WBM base URL and TLS verify flag from firmware version.

    Args:
        firmware_version: Controller firmware string (e.g. "1.9.2").
        host: WBM host from config RestApi Host (typically REST_API_HOST).

    Returns:
        (base_url, verify) where verify is passed to requests.
        FW < 1.9.0 → http://{host}, verify=True
        FW ≥ 1.9.0 → https://{host}, verify=False (self-signed)
    """

    if firmware_at_least(firmware_version, 1, 9, 0):
        return f"https://{host}", False

    return f"http://{host}", True


def resolve_firmware_version(
    telemetry_buffer: Optional[Dict[str, Any]],
    rest_host: str,
    rest_port: int,
) -> Optional[str]:
    """
    Prefer any firmware_version found under telemetry_buffer; fall back to
    GET /api/v1.0/charging-controllers on the localhost REST API.

    Caller should pass a snapshot under TELEMETRY_LOCK, or accept a race.
    """

    if telemetry_buffer:
        for _device_uid, info in telemetry_buffer.items():
            if not isinstance(info, dict):
                continue
            fw = info.get("firmware_version")
            if fw:
                return str(fw)

    url = f"http://{rest_host}:{rest_port}/api/v1.0/charging-controllers"
    response = send_request(url, "GET")
    if response is None or not response.ok:
        logging.warning("Could not resolve firmware version from REST controllers list")
        return None

    try:
        controllers = response.json()
    except Exception as err:
        logging.warning("Failed to parse charging-controllers JSON: %s", err)
        return None

    if not isinstance(controllers, dict):
        return None

    for _device_uid, info in controllers.items():
        if not isinstance(info, dict):
            continue
        fw = info.get("firmware_version")
        if fw:
            return str(fw)

    return None


def login(
    username: str,
    password: str,
    firmware_version: Optional[str],
    host: str,
) -> Optional[str]:
    """
    Log into CHARX WBM and return access_token.

    FW < 1.9.0: POST /api/v1.0/web/login over HTTP
    FW ≥ 1.9.0: POST /api/v1.0/auth/login over HTTPS with verify=False

    Never logs the password.

    Args:
        host: WBM host from config RestApi Host (typically REST_API_HOST).
    """

    # Evaluate FW once so missing/unparseable versions only warn once.
    is_modern = firmware_at_least(firmware_version, 1, 9, 0)

    if is_modern:
        base, verify = f"https://{host}", False
        path = "/api/v1.0/auth/login"
    else:
        base, verify = f"http://{host}", True
        path = "/api/v1.0/web/login"

    url = f"{base}{path}"
    response = send_request(
        url,
        "POST",
        json={"username": username, "password": password},
        verify=verify,
        timeout=30,
    )

    if response is None:
        logging.error("WBM login failed: no response (transport error). URL: %s", url)
        return None

    if not response.ok:
        logging.error(
            "WBM login failed: HTTP %s. URL: %s",
            response.status_code,
            url,
        )
        return None

    try:
        body = response.json()
    except Exception as err:
        logging.error("WBM login: invalid JSON response: %s", err)
        return None

    token = body.get("access_token") if isinstance(body, dict) else None
    if not token:
        logging.error("WBM login: response missing access_token")
        return None

    return str(token)


def download_logs(
    access_token: str,
    firmware_version: Optional[str],
    host: str,
    timeout: int = 300,
) -> Optional[bytes]:
    """
    Download the packaged log archive from CHARX WBM.

    GET /api/v1.0/web/download/logs with Bearer token.
    Uses the same base URL / verify as login for the given firmware.

    Args:
        host: WBM host from config RestApi Host (typically REST_API_HOST).
    """

    base, verify = wbm_base_url(firmware_version, host)
    url = f"{base}/api/v1.0/web/download/logs"

    # Auth-only headers — do not set Content-Type so we do not force JSON.
    headers = {"Authorization": f"Bearer {access_token}"}

    response = send_request(
        url,
        "GET",
        headers=headers,
        verify=verify,
        timeout=timeout,
    )

    if response is None:
        logging.error("WBM log download failed: no response (transport/timeout). URL: %s", url)
        return None

    if response.status_code != 200:
        logging.error(
            "WBM log download failed: HTTP %s. URL: %s",
            response.status_code,
            url,
        )
        return None

    content = response.content
    if content is None:
        logging.error("WBM log download returned empty body")
        return None

    logging.info("WBM log archive downloaded: %s bytes", len(content))
    return content
