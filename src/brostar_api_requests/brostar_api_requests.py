import ast
import csv
import logging
import os
from pathlib import Path
from typing import Literal

import polars as pl
import requests
from dotenv import load_dotenv

from .connection import BROSTARConnection
from .formatter import PayloadFormatter
from .upload_models import GMWConstruction, MonitoringTube, UploadTask, UploadTaskMetadata

logger = logging.getLogger(__name__)
RequestTypeOptions = Literal["registration", "replace", "insert", "move", "delete"]
RegistrationTypeOptions = Literal["GMW_Construction"]

load_dotenv()


def _move_gmw(
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
    )
    payload = payload.model_dump(mode="json", by_alias=True)
    r = brostar.post_upload(payload)
    r.raise_for_status()

    uuid: str = r.json()["uuid"]
    brostar.await_completed(uuid=uuid)


def _correct_gmw(brostar: BROSTARConnection, upload_task: UploadTask) -> None:
    """Send a move request that corrects the dates."""
    payload = upload_task.model_dump(mode="json", by_alias=True)
    print(payload)
    r = brostar.post_upload(payload)
    r.raise_for_status()

    uuid: str = r.json()["uuid"]
    brostar.await_completed(uuid=uuid)


def delete_invalid_upload_tasks() -> None:
    """Delete all upload tasks that are not valid."""
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    next = ""
    while next is not None:
        r = brostar.get("uploadtasks", params={"status": "PROCESSING", "log": "XML is not valid"})
        r.raise_for_status()
        tasks = r.json()["results"]

        for task in tasks:
            uuid = task["uuid"]
            logger.info(f"Deleting invalid upload task {uuid}")
            delete_r = brostar.s.delete(url=f"{brostar.website}/uploadtasks/{uuid}")
            delete_r.raise_for_status()

        next = r.json().get("next")


def bulk_move_request(excel_file: str) -> None:
    """Use an excel to move multiple GMWs.

    Columns: internal_id, gmw, old_date, new_date"""
    # Access your API key
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    df = pl.read_excel(excel_file, has_header=True)
    filtered_df = df.filter(pl.col("gmw").str.starts_with("GMW"))
    filtered_df = filtered_df.filter(pl.col("internal_id").str.ends_with("-1"))
    filtered_df = filtered_df.with_columns(
        pl.col("internal_id").str.strip_suffix("-1").alias("internal_id"),
    )

    formatter = PayloadFormatter(brostar)

    for row in filtered_df.iter_rows(named=True):
        logger.info(row)
        intern_id = row.get("internal_id")
        bro_id = row.get("gmw")
        date_to_be_corrected = row.get("old_date")
        actual_date = row.get("new_date")

        logger.info(f"Moving {bro_id} from {date_to_be_corrected} to {actual_date}")
        construction = formatter.format_gmw_construction(bro_id)
        construction.object_id_accountable_party = intern_id
        construction.well_construction_date = actual_date
        construction.date_to_be_corrected = date_to_be_corrected

        metadata = UploadTaskMetadata(
            request_reference="BROSTAR-API",
            delivery_accountable_party=intern_id,
            quality_regime="IMBRO",
            bro_id=bro_id,
            correction_reason="eigenCorrectie",
        )

        payload = UploadTask(
            bro_domain="GMW",
            project_number="5871",
            registration_type="GMW_Construction",
            request_type="registration",
            sourcedocument_data=construction,
            metadata=metadata,
        )
        payload = payload.model_dump(mode="json", by_alias=True)
        _move_gmw(brostar, construction, metadata)


def map_polars_to_gmw_constructions(df: pl.DataFrame) -> GMWConstruction:
    """
    Maps polars DataFrame to GMWConstruction objects.
    Groups by 'Putnaam' and creates one GMWConstruction per well with associated MonitoringTubes.
    """
    # Create monitoring tubes for this well
    monitoring_tubes = []
    for i, row in enumerate(df.iter_rows(named=True)):
        tube = create_monitoring_tube(row, i + 1)  # tube_number starts at 1
        monitoring_tubes.append(tube)

    first_row = df.row(0, named=True)
    putnaam = first_row.get("Putnaam", "")

    # Create delivered_location from coordinates
    x_coord = first_row.get("X-coordinaat(RD)", "")
    y_coord = first_row.get("Y-coordinaat(RD)", "")
    delivered_location = f"{x_coord} {y_coord}" if x_coord and y_coord else ""

    # Create GMWConstruction
    construction = GMWConstruction(
        # Required fields
        object_id_accountable_party=putnaam,  # Using Putnaam as specified
        nitg_code=str(putnaam)[:-1],
        delivery_context=first_row.get("Kader aanlevering", ""),
        construction_standard=first_row.get("Kwaliteitsnorminrichting", ""),
        initial_function=first_row.get("Initiële functie", ""),
        number_of_monitoring_tubes=len(monitoring_tubes),
        ground_level_stable=first_row.get("Maaiveld stabiel", ""),
        well_stability=first_row.get("Putstabiliteit"),
        # Optional fields with defaults
        owner="17278718",  # Provincie Noord-Brabant
        maintenance_responsible_party="16005077",  # BrabantWater
        well_head_protector=first_row.get("Beschermconstructie", ""),
        well_construction_date=format_date(first_row.get("Inrichtingsdatum")),
        delivered_location=delivered_location,
        horizontal_positioning_method=first_row.get("Method Coordinatenbepaling", ""),
        local_vertical_reference_point="NAP",  # Always NAP as specified
        offset=0.0,  # No mapping available - needs default
        vertical_datum="NAP",  # Always NAP as specified
        ground_level_position=first_row.get("Maaiveldpositie (m+NAP)"),
        ground_level_positioning_method=first_row.get("Method Maaiveldpositiebepaling", ""),
        monitoring_tubes=monitoring_tubes,
    )

    return construction


def create_monitoring_tube(row: dict, tube_number: int) -> MonitoringTube:
    """Creates a MonitoringTube from a row of data."""

    return MonitoringTube(
        tube_number=tube_number,
        tube_type=row.get("BuisType", ""),
        artesian_well_cap_present=row.get("Drukdop", ""),  # Assuming this maps to Drukdop
        sediment_sump_present=row.get("Voorzien van zandvang", ""),
        number_of_geo_ohm_cables=0,  # No data available
        tube_top_diameter=row.get("Diameter bovenkantbuis (mm)"),
        variable_diameter=row.get("Variable diameter"),
        tube_status=row.get("Buis status", ""),
        tube_top_position=row.get("Positie bovenkantbuis (m+NAP)", 0.0),
        tube_top_positioning_method=row.get("MethodePositiebepalingBovenkantbuis", ""),
        tube_packing_material=row.get("Aanvulmaterial buis", ""),
        tube_material=row.get("Materiaal peilbuis", ""),
        glue=row.get("Lijm", ""),
        screen_length=max(row.get("Filterlengte (meters)", 0.5), 0.5),
        screen_protection=None,  # No clear mapping
        sock_material=row.get("Kousmateriaal", ""),
        plain_tube_part_length=max(row.get("Lengte stijgbuisdeel (meters)", 0.5), 0.5),
        sediment_sump_length=row.get("Zandvanglengte (meters)")
        if row.get("Zandvanglengte (meters)")
        else None,
        geo_ohm_cables=None,  # No data available
    )


def format_date(date_value) -> str:
    """Format date value to string. Adjust based on your date format needs."""
    if date_value is None:
        return ""

    # Handle different date formats as needed
    if isinstance(date_value, str):
        return date_value
    elif hasattr(date_value, "strftime"):
        return date_value.strftime("%Y-%m-%d")
    else:
        return str(date_value)


def format_incomplete_date(incomplete_date) -> str | None:
    """Format incomplete date field."""
    if incomplete_date is None or incomplete_date == "":
        return None
    return str(incomplete_date)


def bulk_gmw_tubenumber_correction_request(excel_file: str | Path, kvk: str) -> None:
    """Use an excel to move multiple GMWs.

    Columns: internal_id, gmw, old_date, new_date"""
    # Access your API key
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    df = pl.read_excel(excel_file, has_header=True)
    filtered_df = df.filter(pl.col("gmw_id").str.starts_with("GMW"))

    formatter = PayloadFormatter(brostar)

    for row in filtered_df.iter_rows(named=True):
        logger.info(row)
        bro_id = row.get("gmw_id")

        construction = formatter.format_gmw_construction(bro_id)
        construction.object_id_accountable_party = (
            f"Correctie_{construction.nitg_code if construction.nitg_code else bro_id}"
        )
        construction.monitoring_tubes[0].tube_number = 44

        metadata = UploadTaskMetadata(
            request_reference="20250526_FRE44_Correctie_Gelderland",
            delivery_accountable_party=kvk,
            quality_regime="IMBRO",
            bro_id=bro_id,
            correction_reason="eigenCorrectie",
        )
        upload_task = UploadTask(
            bro_domain="GMW",
            project_number="5459",
            registration_type="GMW_Construction",
            request_type="replace",
            sourcedocument_data=construction,
            metadata=metadata,
        )
        _correct_gmw(brostar, upload_task)


def bulk_gmw_construction_request(excel_file: str | Path, kvk: str) -> None:
    """Use an excel to create multiple GMWs."""
    # Access your API key
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    df = pl.read_excel(excel_file, has_header=True)
    putten = df.unique("Putnaam").to_series(0).to_list()

    for put in putten:
        construction = map_polars_to_gmw_constructions(df.filter(pl.col("Putnaam").eq(put)))
        ### Setup the payload
        metadata = UploadTaskMetadata(
            request_reference=f"{put}",
            delivery_accountable_party=kvk,
            quality_regime="IMBRO",
        )

        ## Extract excel into GMW Construction
        sourcedocument_data = construction

        payload = UploadTask(
            bro_domain="GMW",
            project_number="1497",
            registration_type="GMW_Construction",
            request_type="registration",
            sourcedocument_data=sourcedocument_data,
            metadata=metadata,
        )
        payload = payload.model_dump(mode="json", by_alias=True)
        print(payload)
        r = brostar.post_upload(payload=payload, is_json=True)
        r.raise_for_status()

        uuid: str = r.json()["uuid"]
        brostar.await_completed(uuid=uuid)
    return


def pop_upload_task_fields(upload_task: dict) -> dict:
    """Remove unnecessary fields from the upload task."""
    upload_task.pop("uuid", None)
    upload_task.pop("created_at", None)
    upload_task.pop("updated_at", None)
    upload_task.pop("data_owner", None)
    return upload_task


def clear_fields_for_upload(upload_task: dict) -> dict:
    """Clear fields that should not be set for a new upload task."""
    upload_task["status"] = "PENDING"
    upload_task["log"] = ""
    upload_task["progress"] = 0
    upload_task["bro_id"] = ""
    upload_task["bro_delivery_url"] = ""
    return upload_task


def correct_gld_dossier_for_observation_request(
    current_id: str,
    target_id: str,
):
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    r = brostar.get(
        "uploadtasks",
        params={
            "registration_type": "GLD_Addition",
            "bro_id": current_id,
        },
    )
    for result in r.json().get("results", []):
        result = brostar.get_detail(endpoint="uploadtasks", uuid=result["uuid"]).json()
        result = pop_upload_task_fields(result)
        result = clear_fields_for_upload(result)
        result["request_type"] = "delete"
        result["metadata"].update({"correctionReason": "eigenCorrectie"})
        r = brostar.post_upload(payload=result, is_json=True)
        r.raise_for_status()

        uuid: str = r.json()["uuid"]
        brostar.await_completed(uuid=uuid)

        result["request_type"] = "registration"
        result["metadata"]["requestReference"].replace(current_id, target_id)
        result["metadata"]["broId"] = target_id
        result["metadata"].pop("correctionReason")
        result["status"] = "PENDING"
        r = brostar.post_upload(payload=result, is_json=True)
        r.raise_for_status()

        uuid: str = r.json()["uuid"]
        brostar.await_completed(uuid=uuid)


def convert_to_list(s):
    return ast.literal_eval(s)


def correct_bulk_gld(excel_file: str | Path) -> None:
    df = pl.read_excel(excel_file, has_header=True)
    df_converted = df.with_columns(
        pl.col("broId").map_elements(convert_to_list, return_dtype=pl.List(pl.String))
    )
    # Extract first value as 'correct_id' and explode the rest as 'target_id'
    result = (
        df_converted.with_columns(
            [
                pl.col("broId").list.first().alias("target_id"),
                pl.col("broId").list.slice(1).alias("current_ids"),
            ]
        )
        .drop("broId")
        .explode("current_ids")
        .rename({"current_ids": "current_id"})
    )
    total = result.height
    logger.info(f"Total rows to process: {total}")
    skip_count = 0
    delete_ids = []
    for i, row in enumerate(result.iter_rows(named=True)):
        logger.info(f"Processing row {i + 1}/{total}: {row}")
        r = requests.get(
            f"https://publiek.broservices.nl/gm/gld/v1/objects/{row['current_id']}/observationsSummary"
        )
        if len(r.json()) == 0:
            skip_count += 1
            logger.info(f"No observations found for {row['current_id']}. Skipping.")
            delete_ids += [row["current_id"]]
            continue

        correct_gld_dossier_for_observation_request(
            current_id=row["current_id"],
            target_id=row["target_id"],
        )
        logger.info(f"Completed processing row {i + 1}/{total}")
        delete_ids += [row["current_id"]]

    print(f"Skipped {skip_count} rows due to no observations found.")

    # Write to a CSV file
    with open(
        r"C:\Users\steven.hosper\Downloads\delete_ids.csv", mode="w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(["broId"])  # Header
        for bro_id in delete_ids:
            writer.writerow([bro_id])


def process_result(result: dict) -> None:
    lizard_api_key = os.getenv("LIZARD_API_KEY")
    lizard_s = requests.Session()
    lizard_s.headers = {
        "username": "__key__",
        "password": lizard_api_key,  # Lizard API Key
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
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
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
