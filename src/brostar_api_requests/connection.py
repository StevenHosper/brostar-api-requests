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


class BROSTARConnection:
    def __init__(self, token: str):
        if not isinstance(token, str):
            raise ValueError("Token must be a string.")

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
        print(self.s.auth.username, self.s.auth.password)
        return self.s.get(url=f"{self.website}/{endpoint}/", params=params, timeout=15)

    def get_detail(self, endpoint: BrostarEndpoint, uuid: str) -> requests.Response:
        return self.s.get(url=f"{self.website}/{endpoint}/{uuid}", timeout=15)

    def post_upload(self, payload: dict[str, str], is_json: bool = True) -> requests.Response:
        print(payload)
        if is_json:
            return self.s.post(url=f"{self.website}/uploadtasks/", json=payload, timeout=15)
        return self.s.post(url=f"{self.website}/uploadtasks/", data=payload, timeout=15)

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
        r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=15)
        r.raise_for_status()
        bro_id = r.json().get("bro_id", None)
        while bro_id is None and timer < 45:
            time.sleep(3)
            r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=15)
            r.raise_for_status()
            bro_id = r.json().get("bro_id", None)
            timer += 3

        return bro_id

    def await_completed(self, uuid: str) -> requests.Response:
        """
        Wait for the bro_id to be available in the response. For a maximum of 45 seconds. Then return None.
        Input: uuid of uploadtask.
        Output: bro_id or None.
        """
        timer = 0
        r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=15)
        r.raise_for_status()
        status = r.json().get("status", "PENDING")
        while status != "COMPLETED" and timer < 45:
            time.sleep(3)
            try:
                r = self.s.get(url=f"{self.website}/uploadtasks/{uuid}/", timeout=15)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.exception(f"Error while checking status: {e}")
                timer += 3
                continue

            status = r.json().get("status", "PENDING")
            timer += 3

        return r

    def check_status(self, uuid: str) -> requests.Response:
        return self.s.post(url=f"{self.website}/uploadtasks/{uuid}/check_status/", timeout=15)
