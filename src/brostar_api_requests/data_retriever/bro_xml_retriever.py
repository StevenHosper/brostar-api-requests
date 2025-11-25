import json
import time

import requests


class BROID_Retriever:
    def __init__(self, base_url: str, kvk_number: str) -> None:
        if not isinstance(kvk_number, str):
            raise TypeError("KVK-number should be string.")
        if not kvk_number.isdigit():
            raise ValueError("KVK-number should only contain digits.")

        self.base_url = base_url
        self.kvk_number = kvk_number

    def _request_bro_ids(self, type: str) -> list | None:
        options = ["gmw", "frd", "gar", "gmn", "gld"]
        if type.lower() not in options:
            raise Exception(f"Unknown type: {type}. Use a correct option: {options}.")

        retry = 0
        while retry < 3:
            res = requests.get(
                f"{self.base_url}/gm/{type}/v1/bro-ids?bronhouder={self.kvk_number}&registered=ja"
            )
            if res.status_code < 300:
                return json.loads(res.text)["broIds"]

            retry += 1
            time.sleep(15)

        return None

    def gmw_ids(self):
        return self._request_bro_ids("gmw")

    def gld_ids(self):
        return self._request_bro_ids("gld")

    def gmn_ids(self):
        return self._request_bro_ids("gmn")

    def frd_ids(self):
        return self._request_bro_ids("frd")

    def gar_ids(self):
        return self._request_bro_ids("gar")
