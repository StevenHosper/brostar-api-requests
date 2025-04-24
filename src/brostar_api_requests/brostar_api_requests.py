import logging
from typing import Literal

import polars as pl
import requests

from .connection import BROSTARConnection
from .formatter import PayloadFormatter
from .upload_models import GMWConstruction, UploadTask, UploadTaskMetadata

logger = logging.getLogger(__name__)
RequestTypeOptions = Literal["registration", "replace", "insert", "move", "delete"]
RegistrationTypeOptions = Literal["GMW_Construction"]


def move_gmw(
    brostar: BROSTARConnection, construction: GMWConstruction, metadata: UploadTaskMetadata
) -> None:
    """Send a move request that corrects the dates."""
    payload = UploadTask(
        bro_domain="GMW",
        project_number="5871",
        registration_type="GMW_Construction",
        request_type="move",
        sourcedocument_data=construction,
        metadata=metadata,
    ).model_dump(mode="json")

    r = brostar.post_upload(payload)
    uuid: str = r.json()["uuid"]
    brostar.await_completed(uuid=uuid)


def bulk_move_request(excel_file: str) -> None:
    brostar = BROSTARConnection("")  # BROSTAR API Key
    brostar.set_website(production=True)

    df = pl.read_excel(excel_file, has_header=True)
    filtered_df = df.filter(pl.col("gmw").str.starts_with("GMW"))
    formatter = PayloadFormatter(brostar)

    for row in filtered_df.iter_rows(named=True):
        logger.info(row)
        bro_id = row["gmw"]
        date_to_be_corrected = row["origineel"]
        actual_date = row["correct"]
        logger.info(f"Moving {bro_id} from {date_to_be_corrected} to {actual_date}")
        construction, metadata = formatter.format_gmw_construction(bro_id)
        construction.maintenanceResponsibleParty = "01174741"
        construction.wellConstructionDate = actual_date
        construction.dateToBeCorrected = date_to_be_corrected
        move_gmw(brostar, construction, metadata)


def main():
    file_path = r"C:\Users\steven.hosper\Desktop\PythonPackages\BrostarAPI\20250401 Nieuw inrichtingsdatums put voor GMW.xlsx"
    bulk_move_request(file_path)


def process_result(result: dict) -> None:
    lizard_s = requests.Session()
    lizard_s.headers = {
        "username": "__key__",
        "password": "",  # Lizard API Key
        "Content-Type": "application/json",
    }

    r = lizard_s.get(
        url="https://rotterdam.lizard.net/api/v4/locations/",
        params={
            "code": f"{result['sourcedocument_data']['gmwBroId']}-{result['sourcedocument_data']['tubeNumber']:03d}"
        },
        timeout=15,
    )
    r.raise_for_status()
    if len(r.json()["results"]) == 0:
        logger.info("No locations found.")
        return

    extra_metadata = r.json()["results"][0]["extra_metadata"]
    logger.info(f"quality_regime is {result['metadata']['qualityRegime']}")
    logger.info(f"BRO-ID: {result['bro_id']}")

    if result["metadata"]["qualityRegime"] == "IMBRO":
        extra_metadata["bro"]["gldIdImbro"] = result["bro_id"]
        logger.info(extra_metadata["bro"])
    else:
        extra_metadata["bro"]["gldIdImbroA"] = result["bro_id"]
        logger.info(extra_metadata["bro"])

    r = lizard_s.patch(
        url=r.json()["results"][0]["url"], json={"extra_metadata": extra_metadata}, timeout=15
    )
    r.raise_for_status()
    print(r.json())
    print("\n\n")


def ingest_gld_ids_into_lizard():
    """Retrieve all uploadtasks / registrations and ingest the information into Lizard."""
    brostar = BROSTARConnection()  # BROSTAR API Key
    brostar.set_website(production=True)

    r = brostar.get(
        "uploadtasks", params={"registration_type": "GLD_StartRegistration", "status": "COMPLETED"}
    )
    while r.json()["next"] is not None:
        for result in r.json()["results"]:
            logger.info(f"Processing {result}")
            # Get the bro_id from the registration and update Lizard
            process_result(result)

        r = brostar.s.get(url=r.json()["next"], timeout=15)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
