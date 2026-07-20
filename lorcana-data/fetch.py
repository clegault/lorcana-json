"""Ravensburger API authentication and catalog download."""

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# db.lorcanito.com was retired; the card/ability data now lives behind the
# tcg.online JSON API (Scryfall-style, paginated). A browser-like User-Agent
# is required or Cloudflare returns 403.
LORCANITO_URL = "https://api.tcg.online/v1/lorcana/cards"
_TCG_UA = "Mozilla/5.0 (lorcana-json data pipeline)"

SSO_URL = "https://sso.ravensburger.de/token"
CATALOG_URL = "https://api.lorcana.ravensburger.com/v3/catalog/{lang}"

# Base64-encoded client_credentials (Basic auth) for the Ravensburger API
_CREDENTIAL = (
    "bG9yY2FuYS1hcGktcmVhZDpFdkJrMzJkQWtkMzludWt5QVNIMHc2X2FJcVZEcHpJen"
    "VrS0lxcDlBNXRlb2c5R3JkQ1JHMUFBaDVSendMdERkYlRpc2k3THJYWDl2Y0FkSTI4"
    "S096dw=="
)

_token: str | None = None


def _get_token() -> str:
    global _token
    if _token:
        return _token
    resp = requests.post(
        SSO_URL,
        headers={"Authorization": f"Basic {_CREDENTIAL}"},
        data={"grant_type": "client_credentials"},
        verify=False,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    _token = f"{data['token_type']} {data['access_token']}"
    return _token


def download_catalog(lang: str) -> dict:
    """Download the full catalog for one language. Returns parsed JSON."""
    token = _get_token()
    resp = requests.get(
        CATALOG_URL.format(lang=lang),
        headers={"Authorization": token},
        verify=False,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_lorcanito() -> list:
    """Fetch every card from the tcg.online API and return the flat list.

    The endpoint paginates (100 cards/page); we walk pages until has_more is
    false. Each card is a Scryfall-style dict with set/number/abilities, which
    lorcanito.py indexes for ability resolution.
    """
    result: list = []
    page = 1
    while True:
        resp = requests.get(
            LORCANITO_URL,
            params={"page": page},
            headers={"User-Agent": _TCG_UA},
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()
        result.extend(payload.get("data", []))
        if not payload.get("has_more"):
            break
        page += 1

    if not result:
        raise ValueError("tcg.online returned no cards")

    return result
