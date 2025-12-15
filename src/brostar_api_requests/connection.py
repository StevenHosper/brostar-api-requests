import base64
import json
import logging
import time
from typing import BinaryIO, Literal

import requests
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

BrostarEndpoint = Literal[
    "users",
    "organisations",
    "importtasks",
    "uploadtasks",
    "bulkuploads",
    "gmn/gmns",
    "gmn/measuringpoints",
    "gmw/gmws",
    "gmw/monitoringtubes",
    "gmw/events",
    "gar/gars",
    "gld/glds",
    "gld/observations",
    "frd/frds",
]

BrostarUploadEndpoints = Literal[
    "importtasks",
    "uploadtasks",
    "bulkuploads",
]

BroRequest = Literal["registration", "replace", "insert", "move", "delete"]


def decode_jwt(token):
    """Decode a JWT without checking its signature"""
    # JWT consists of {header}.{payload}.{signature}
    _, payload, _ = token.split(".")
    # JWT should be padded with = (base64.b64decode expects this)
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.b64decode(payload))


def is_token_usable(token: str, leeway: int) -> bool:
    """Determine whether the token has expired"""
    try:
        claims = decode_jwt(token)
    except Exception:
        return False

    exp = claims["exp"]
    refresh_on = exp - leeway
    return refresh_on >= int(time.time())


class BROSTARConnection:
    def __init__(self, token: str | None = None) -> None:
        # Session
        self.website = "https://staging.brostar.nl/api"
        self.s = requests.Session()
        retry = Retry(
            total=6,
            backoff_factor=0.5,
        )
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry)
        self.s.mount("http://", adapter)
        self.s.mount("https://", adapter)

        if token is None:
            logger.info("Unauthenticated session created.")
            return

        if not isinstance(token, str):
            raise ValueError("Token must be a string.")

        self.authenticate(token)

    def set_website(self, production: bool) -> None:
        """
        Set the website to production or staging.
        :param production: True for production, False for staging.
        """
        if production:
            self.website = "https://www.brostar.nl/api"
            logger.info("Production set.")
        else:
            self.website = "https://staging.brostar.nl/api"
            logger.info("Staging set.")

    def refresh_access_token(self, client_id: str, client_secret: str) -> None:
        """
        Retrieve and set access token using OAuth2 client credentials grant.
        :param client_id: OAuth2 client ID.
        :param client_secret: OAuth2 client secret.
        """
        auth = HTTPBasicAuth(
            username=client_id,
            password=client_secret,
        )

        # Client credentials grant uses the token endpoint, not authorize
        token_url = "https://auth.lizard.net/oauth2/token"

        # Prepare the request
        data = {"grant_type": "client_credentials"}

        # Make POST request to token endpoint
        r = self.s.post(url=token_url, auth=auth, data=data, timeout=30)

        r.raise_for_status()  # Raise exception for bad status codes

        # Parse the response
        token_data = r.json()
        access_token = token_data.get("access_token")

        if not access_token:
            raise ValueError("No access token received from authorization server")

        # Set the access token in session headers
        self.s.headers.update({"Authorization": f"Bearer {access_token}"})

        logger.info("Access token retrieved and set successfully.")

    def check_token_validity(self, leeway: int = 60) -> bool:
        """
        Check if the current token is still valid.
        :param leeway: Time in seconds before expiry to consider the token invalid.
        :return: True if token is valid, False otherwise.
        """
        auth_header = self.s.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            logger.warning("No Bearer token found in headers.")
            return False

        token = auth_header.split(" ")[1]
        is_valid = is_token_usable(token, leeway)
        if is_valid:
            logger.info("Token is valid.")
        else:
            logger.warning("Token has expired or is invalid.")
        return is_valid

    def authenticate(self, token: str) -> None:
        """
        Set headers for the session.
        :param token: Token to be used in the headers.
        """
        auth = HTTPBasicAuth(
            username="__key__",
            password=token,
        )
        self.s.auth = auth
        logger.info("Authentication set.")

    def get(self, endpoint: BrostarEndpoint, params: dict | None = None) -> requests.Response:
        return self.s.get(url=f"{self.website}/{endpoint}/", params=params, timeout=30)

    def get_detail(self, endpoint: BrostarEndpoint, uuid: str) -> requests.Response:
        return self.s.get(url=f"{self.website}/{endpoint}/{uuid}", timeout=30)

    def post_upload(self, payload: dict[str, str], is_json: bool = True) -> requests.Response:
        if is_json:
            return self.s.post(url=f"{self.website}/uploadtasks/", json=payload, timeout=30)
        return self.s.post(url=f"{self.website}/uploadtasks/", data=payload, timeout=30)

    def post_gar_bulk(
        self, payload: dict[str, str], fieldwork_file: BinaryIO, lab_file: BinaryIO
    ) -> requests.Response:
        return self.s.post(
            url=f"{self.website}/bulkuploads/",
            data=payload,
            files={"fieldwork_file": fieldwork_file, "lab_file": lab_file},
            timeout=60,
        )

    def post_gmn_bulk(
        self, payload: dict[str, str], measuring_point_file: BinaryIO
    ) -> requests.Response:
        return self.s.post(
            url=f"{self.website}/bulkuploads/",
            data=payload,
            files={"measurement_tvp_file": measuring_point_file},
            timeout=30,
        )

    def post_gld_bulk(
        self, payload: dict[str, str], timeseries_file: BinaryIO
    ) -> requests.Response:
        return self.s.post(
            url=f"{self.website}/bulkuploads/",
            data=payload,
            files={"measurement_tvp_file": timeseries_file},
            timeout=60,
        )

    def await_bro_id(self, uuid: str) -> str | None:
        """
        Wait for the bro_id to be available in the response. For a maximum of 45 seconds. Then return None.
        Input: uuid of uploadtask.
        Output: bro_id or None.
        """
        timer = 0
        r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=30)
        r.raise_for_status()
        bro_id = r.json().get("bro_id", None)
        while bro_id is None and timer < 10:
            time.sleep(3)
            r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=30)
            r.raise_for_status()
            bro_id = r.json().get("bro_id", None)
            timer += 3

        return bro_id

    def await_completed(self, uuid: str) -> requests.Response:
        """
        Wait for the bro_id to be available in the response. For a maximum of 15 seconds. Then return None.
        Input: uuid of uploadtask.
        Output: bro_id or None.
        """
        timer = 0
        r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=30)
        r.raise_for_status()
        status = r.json().get("status", "PENDING")
        while status != "COMPLETED" and timer <= 15:
            time.sleep(3)
            try:
                r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=30)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.exception(f"Error while checking status: {e}")
                timer += 3
                continue

            status = r.json().get("status", "PENDING")
            timer += 3

        logger.warning(f"Final status: {status}")
        return r

    def check_status(self, uuid: str) -> requests.Response:
        return self.s.post(url=f"{self.website}/uploadtasks/{uuid}/check_status/", timeout=30)


def setup_brostar_connection(token: str, production: bool = False) -> BROSTARConnection:
    brostar = BROSTARConnection(token)
    brostar.set_website(production=production)
    return brostar
