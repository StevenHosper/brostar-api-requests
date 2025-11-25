import logging
import os

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)


def setup_lizard_session() -> requests.Session:
    lizard_api_key = os.getenv("LIZARD_API_KEY")
    ls = requests.Session()
    ls.headers = {
        "username": "__key__",
        "password": lizard_api_key,
        "Content-Type": "application/json",
    }
    retry = Retry(
        total=6,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        connect=6,
        read=6,
    )
    adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry)
    ls.mount("http://", adapter)
    ls.mount("https://", adapter)
    return ls


def update_lizard(code: str, extra_metadata_change: dict) -> requests.Response | None:
    s = setup_lizard_session()
    url = f"https://vitens.lizard.net/api/v4/locations/?code={code}"

    try:
        response = s.get(url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error fetching location {code}: {e}")
        return None

    results = response.json().get("results", [])
    if len(results) == 0:
        logger.error(f"No location found with code {code}")
        return None

    item = results[0]
    extra_metadata: dict = item.get("extra_metadata", {})
    extra_metadata.update(extra_metadata_change)

    r = s.patch(item["url"], json={"extra_metadata": extra_metadata})
    r.raise_for_status()
    return r
