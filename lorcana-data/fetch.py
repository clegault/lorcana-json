"""Ravensburger API authentication and catalog download."""

import json
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LORCANITO_URL = "https://db.lorcanito.com/cards"

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


def _find_cards(element):
    """Recursively locate the cards array inside the lorcanito RSC payload."""
    if isinstance(element, list):
        for item in element:
            found = _find_cards(item)
            if found is not None:
                return found
    elif isinstance(element, dict):
        if "loading" in element:
            return _find_cards(element["loading"])
        elif "children" in element:
            return _find_cards(element["children"])
        elif "cards" in element and isinstance(element["cards"], list):
            return element["cards"]
    return None


def fetch_lorcanito() -> list:
    """Scrape db.lorcanito.com/cards and return the resolved card list."""
    resp = requests.get(LORCANITO_URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    marker = 'self.__next_f.push([1,"5:'
    idx = html.find(marker)
    if idx == -1:
        raise ValueError("Could not find lorcanito card data payload in page")

    right = html[idx + len(marker):]
    end = right.find('\\n\"])</script>')
    if end == -1:
        raise ValueError("Could not find end marker for lorcanito card data")

    chunk = right[:end].replace('\\\\\\"', "``").replace("\\", "")
    parsed = json.loads(chunk)

    cards_raw = _find_cards(parsed)
    if not cards_raw:
        raise ValueError("Could not locate cards array in lorcanito payload")

    # Top-level entries can be reference strings pointing to an earlier index
    result = []
    for item in cards_raw:
        if isinstance(item, dict):
            result.append(item)
        elif isinstance(item, str):
            ref_idx = int(item.split(":")[-1])
            result.append(result[ref_idx])

    return result
