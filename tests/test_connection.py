import io

import pytest
import requests_mock

from ..src.brostar_api_requests import (
    BROSTARConnection,  # Replace 'your_module' with actual module name
)


@pytest.fixture
def brostar():
    return BROSTARConnection(token="test-token")


def test_authentication_headers():
    brostar = BROSTARConnection(token="test-token")
    assert brostar.s.auth.username == "__key__"
    assert brostar.s.auth.password == "test-token"


def test_set_website_staging(brostar: BROSTARConnection):
    brostar.set_website(False)
    assert brostar.website == "https://staging.brostar.nl/api"


def test_set_website_production(brostar: BROSTARConnection):
    brostar.set_website(True)
    assert brostar.website == "https://www.brostar.nl/api"


def test_get_endpoint(brostar: BROSTARConnection):
    with requests_mock.Mocker() as m:
        m.get("https://staging.brostar.nl/api/users/", json={"data": "ok"})
        res = brostar.get("users")
        assert res.status_code == 200
        assert res.json() == {"data": "ok"}


def test_get_detail(brostar: BROSTARConnection):
    with requests_mock.Mocker() as m:
        m.get("https://staging.brostar.nl/api/users/abc123", json={"id": "abc123"})
        res = brostar.get_detail("users", "abc123")
        assert res.status_code == 200
        assert res.json()["id"] == "abc123"


def test_post_upload(brostar: BROSTARConnection):
    with requests_mock.Mocker() as m:
        m.post("https://staging.brostar.nl/api/uploadtasks/", status_code=201)
        res = brostar.post_upload(payload={"some": "data"})
        assert res.status_code == 201


def test_post_gar_bulk(brostar: BROSTARConnection):
    with requests_mock.Mocker() as m:
        m.post("https://staging.brostar.nl/api/bulkuploads/", status_code=201)

        fieldwork_file = ("fieldwork.csv", io.BytesIO(b"col1,col2\nval1,val2"), "text/csv")
        lab_file = ("lab.csv", io.BytesIO(b"labcol1,labcol2\nlabval1,labval2"), "text/csv")

        res = brostar.post_gar_bulk(
            {"meta": "data"}, fieldwork_file=fieldwork_file, lab_file=lab_file
        )
        assert res.status_code == 201


def test_post_gmn_bulk(brostar: BROSTARConnection):
    with requests_mock.Mocker() as m:
        m.post("https://staging.brostar.nl/api/bulkuploads/", status_code=201)

        measuring_point_file = (
            "measuring_point_file.csv",
            io.BytesIO(b"col1,col2\nval1,val2"),
            "text/csv",
        )

        res = brostar.post_gmn_bulk({"meta": "data"}, measuring_point_file=measuring_point_file)
        assert res.status_code == 201


def test_post_gld_bulk(brostar: BROSTARConnection):
    with requests_mock.Mocker() as m:
        m.post("https://staging.brostar.nl/api/bulkuploads/", status_code=201)

        measuring_point_file = (
            "measuring_point_file.csv",
            io.BytesIO(b"col1,col2\nval1,val2"),
            "text/csv",
        )

        res = brostar.post_gld_bulk({"meta": "data"}, timeseries_file=measuring_point_file)
        assert res.status_code == 201


def test_await_bro_id_found(monkeypatch):
    call_count = {"count": 0}

    def mock_get(*args, **kwargs):
        call_count["count"] += 1

        class MockResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"bro_id": "bro123"} if call_count["count"] > 1 else {"bro_id": None}

        return MockResp()

    brostar = BROSTARConnection("token")
    brostar.s.get = mock_get
    bro_id = brostar.await_bro_id("some-uuid")
    assert bro_id == "bro123"
    assert call_count["count"] > 1


def test_await_bro_id_timeout(monkeypatch):
    def mock_get(*args, **kwargs):
        class MockResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"bro_id": None}

        return MockResp()

    brostar = BROSTARConnection("token")
    brostar.s.get = mock_get

    # Monkeypatch sleep to fast-forward time
    monkeypatch.setattr("time.sleep", lambda x: None)
    bro_id = brostar.await_bro_id("some-uuid")
    assert bro_id is None


def test_check_status(brostar: BROSTARConnection):
    with requests_mock.Mocker() as m:
        m.post("https://staging.brostar.nl/api/uploadtasks/abc123/check_status/", status_code=200)
        res = brostar.check_status("abc123")
        assert res.status_code == 200
