import ast
import csv
import datetime
import logging
import os
import time
from pathlib import Path
from typing import Literal

import polars as pl
import pytz
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

from .connection import BROSTARConnection
from .formatter import PayloadFormatter
from .upload_models import (
    GLDAddition,
    GMWConstruction,
    MonitoringTube,
    UploadTask,
    UploadTaskMetadata,
)

logger = logging.getLogger(__name__)
RequestTypeOptions = Literal["registration", "replace", "insert", "move", "delete"]
RegistrationTypeOptions = Literal["GMW_Construction"]
AMS_TZ = pytz.timezone("Europe/Amsterdam")
CHUNK_SIZE = 7000
VALIDATION_MAPPING = {
    "goedgekeurd": 2,
    "onbeslist": 5,
    "afgekeurd": 8,
    "nogNietBeoordeeld": 100,
    "onbekend": 200,
    # Any above 100 are corrected values
}

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
    )
    adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry)
    ls.mount("http://", adapter)
    ls.mount("https://", adapter)
    return ls


def post_timeseries_events(
    timeseries_url: str, events_df: pl.DataFrame, session: requests.Session
) -> None:
    """Post timeseries events to lizard with adjusted flag"""
    logger.info(f"Posting timeseries to {f'{timeseries_url}events/'}.")
    logger.info(events_df)
    r = session.post(
        url=f"{timeseries_url}events/",
        json=events_df.to_dicts(),
        timeout=30,
    )
    r.raise_for_status()


def create_brostar_task(url: str, payload: dict, brostar_s: requests.Session) -> dict:
    r = brostar_s.post(url, json=payload, timeout=60)
    print(r.url)
    print(r.json())
    if r.status_code < 250:
        time.sleep(10)
        res = brostar_s.get(r.json()["url"], timeout=30)
        res.raise_for_status()
    else:
        res = r
    return r.json()


def check_status(url: str, brostar_s: requests.Session) -> dict:
    brostar_s.post(f"{url}check_status/")
    r = brostar_s.get(url, timeout=15)
    r.raise_for_status()
    return r.json()


def determine_status_quality_control(value: int | None) -> str:
    if value is None:
        return "nogNietBeoordeeld"

    # Sort the items by their value
    sorted_items = sorted(VALIDATION_MAPPING.items(), key=lambda item: item[1])

    # Iterate over sorted items
    for key, threshold in sorted_items:
        if value < threshold:
            return key

    # If no valid key is found, return a default value (optional)
    return "Invalid value"


def determine_censor_reason(detection_limit: str):
    if detection_limit == ">":
        return "groterDanLimietwaarde"
    elif detection_limit == "<":
        return "kleinerDanLimietwaarde"
    else:
        return "onbekend"


def convert_timeaware_to_bro_str(datetime_val: datetime.datetime) -> str:
    datetime_str = datetime_val.strftime("%Y-%m-%dT%H:%M:%S%z")
    return datetime_str[:22] + ":" + datetime_str[22:]


def setup_time_value_pairs(events_df: pl.DataFrame, limits: dict[str, str]) -> list[dict[str, str]]:
    """Transforms the event_df (lizard format) to BROSTAR (BRO) format."""
    brostar_data_list = []

    events_df = events_df.with_columns(
        pl.col("datetime")
        .dt.replace_time_zone(time_zone="UTC", non_existent="null")
        .dt.convert_time_zone(time_zone="Europe/Amsterdam")
        .alias("datetime"),
    )
    events_df = events_df.with_columns(
        pl.col("datetime")
        .map_elements(convert_timeaware_to_bro_str, return_dtype=pl.String)
        .alias("datetime"),
    )

    for row in events_df.iter_rows(named=True):
        if row["value"] in [None, "None"]:
            value = None
        else:
            value = row["value"]

        brostar_data = {
            "time": row["datetime"],
            "value": value,
            "statusQualityControl": determine_status_quality_control(row["flag"]),
        }

        if brostar_data["statusQualityControl"] == "afgekeurd" and value in ["", None]:
            brostar_data["censorReason"] = determine_censor_reason(row["detection_limit"])
        elif brostar_data["value"] is None:
            brostar_data["censorReason"] = "onbekend"
        else:
            brostar_data["censorReason"] = None

        if brostar_data["censorReason"] in [
            "groterDanLimietwaarde",
            "kleinerDanLimietwaarde",
        ]:
            brostar_data["censorLimit"] = (
                limits["referenceLevel"]
                if brostar_data["censorReason"] == "groterDanLimietwaarde"
                else limits["filterBottomLevel"]
            )

        brostar_data_list.append(brostar_data)

    return brostar_data_list


def send_gldaddition_for_vitens_location(business_id: str, kvk: str, projectnummer: str) -> None:
    """The GLD-ID should be available within the location metadata of the Lizard API. Otherwise this function will fail. For now this only works with IMBRO, as that was the purpose for the function."""
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    ls = setup_lizard_session()

    # Fetch the location metadata from the Lizard API
    r = ls.get(
        url="https://vitens.lizard.net/api/v4/locations/", params={"code__startswith": business_id}
    )
    r.raise_for_status()
    locations = r.json().get("results", [])
    for location in locations:
        logger.info(f"Processing location: {location}")
        location_metadata = location.get("extra_metadata", {}).get("bro", {})
        limits = {
            "referenceLevel": location_metadata.get("temporal_data", [{}])[0].get(
                "referenceLevel", None
            ),
            "filterBottomLevel": location_metadata.get("filterBottomDepth", None),
        }
        gld_id_imbro = location_metadata.get("broid_gld_imbro", None)
        quality_regime = "IMBRO"
        if gld_id_imbro is None:
            logger.info(f"No GLD ID found for location {location['code']}. Skipping.")
            continue

        for observation_type in [28, 911]:
            r = ls.get(
                url="https://vitens.lizard.net/api/v4/timeseries/",
                params={"location__code": location["code"], "observation_type": observation_type},
            )
            r.raise_for_status()
            timeseries = r.json().get("results", [])
            if len(timeseries) != 1:
                logger.info(
                    f"No timeseries found for location {location['code']} and observation type {observation_type}. Skipping."
                )
                continue

            timeserie_info = timeseries[0]
            logger.info(f"Processing timeseries: {timeserie_info}")
            procedures = timeserie_info["extra_metadata"].get("bro", {}).get("procedure", [])
            if not procedures:
                logger.info(
                    f"No procedures found for timeseries {timeserie_info['code']}. Skipping."
                )
                continue
            elif isinstance(procedures, dict):
                procedures = [procedures]

            procedures_df = pl.DataFrame(procedures)
            procedures_df = procedures_df.with_columns(
                pl.col("start")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ")
                .alias("start_datetime"),
                pl.col("eind")
                .str.replace("None", "5000-01-01T00:00:00Z")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ")
                .alias("eind_datetime"),
            )
            logger.info(procedures_df)

            r = ls.get(
                f"{timeserie_info['url']}events/", params={"validation_code!": "V", "limit": 10000}
            )
            r.raise_for_status()
            events = r.json().get("results", [])

            while r.json().get("next") is not None:
                r = ls.get(r.json().get("next"))
                r.raise_for_status()
                events += r.json().get("results", [])

            events_df = pl.DataFrame(events, schema_overrides={"value": pl.Float64})
            events_df = events_df.filter(pl.col("value").is_not_null())
            events_df = events_df.with_columns(
                pl.col("time").str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ").alias("datetime")
            )

            for procedure in procedures_df.iter_rows(named=True):
                logger.info(f"Processing procedure: {procedure}")
                procedure_events_df = events_df.filter(
                    pl.col("datetime").is_between(
                        procedure["start_datetime"],
                        procedure["eind_datetime"],
                    ),
                    pl.col("value").is_not_null(),
                )
                logger.info(procedure_events_df)

                n_rows = procedure_events_df.height  # or len(timeseries_df)

                logger.info(procedure)
                for i in range(0, n_rows, CHUNK_SIZE):
                    chunk = procedure_events_df.slice(i, CHUNK_SIZE)

                    observatie_type = procedure["observationtype"]
                    proces_referentie = procedure["processreference"]
                    evaluatie_procedure = procedure["evaluationprocedure"]
                    meetinstrument_type = procedure["measurementinstrumenttype"]
                    luchtdrukcompensatie = (
                        procedure["airpressurecompensationtype"]
                        if procedure["airpressurecompensationtype"] not in [None, "geen", ""]
                        else None
                    )
                    logger.info(chunk)

                    metadata = UploadTaskMetadata(
                        bro_id=gld_id_imbro,
                        request_reference=f"{gld_id_imbro}: {quality_regime} {observatie_type} {procedure['start']}-{procedure['eind']} ({datetime.datetime.now(tz=AMS_TZ).strftime('%Y-%m-%dT%H:%M:%SZ')})",
                        delivery_accountable_party=kvk,
                        quality_regime="IMBRO",
                    )

                    time_value_pairs = setup_time_value_pairs(chunk, limits)
                    start_time = time_value_pairs[0]["time"]
                    end_time = time_value_pairs[-1]["time"]
                    result_time = time_value_pairs[-1]["time"]  # Only do voorlopig and controle

                    sourcedocument_data = GLDAddition(
                        date=result_time.split("T")[0],
                        investigator_kvk=kvk,
                        validation_status="voorlopig"
                        if observatie_type == "reguliereMeting"
                        else None,
                        observation_type=observatie_type,
                        evaluation_procedure=evaluatie_procedure,
                        process_reference=proces_referentie,
                        measurement_instrument_type=meetinstrument_type,
                        air_pressure_compensation_type=luchtdrukcompensatie,
                        begin_position=start_time.split("T")[0],
                        end_position=end_time.split("T")[0],
                        result_time=result_time,
                        time_value_pairs=time_value_pairs,
                    )

                    payload = UploadTask(
                        bro_domain="GLD",
                        project_number=str(projectnummer),
                        registration_type="GLD_Addition",
                        request_type="registration",
                        sourcedocument_data=sourcedocument_data,
                        metadata=metadata,
                    )

                    # Create delivery
                    try:
                        result_dict: dict = create_brostar_task(
                            url=f"{brostar.website}/uploadtasks/",
                            payload=payload.model_dump(mode="json", by_alias=True),
                            brostar_s=brostar.s,
                        )
                    except Exception as e:
                        logger.exception(
                            f"Failed to post addition: {e}. Payload was: {payload.model_dump(mode='json', by_alias=True)}"
                        )
                        continue

                    # Check delivery
                    retry = 0
                    while (
                        result_dict.get("status", "UNKNOWN") in ["PROCESSING", "PENDING"]
                        and retry < 5
                    ):
                        try:
                            result_dict = check_status(result_dict["url"], brostar_s=brostar.s)
                        except Exception as e:
                            logger.exception(f"Failed to check the status at brostar: {e}.")

                        retry += 1
                        time.sleep(5)

                    # Update last delivered date
                    if result_dict["status"] in ["COMPLETED", "UNFINISHED"]:
                        url = timeserie_info["url"]
                        chunk = chunk.with_columns(pl.lit("V").alias("validation_code"))
                        # Convert datetime to str (JSON-Serializeable)
                        chunk = chunk.select(
                            "time",
                            "value",
                            "validation_code",
                            "detection_limit",
                            "flag",
                            "comment",
                            "last_modified",
                        )
                        post_timeseries_events(url, chunk, ls)


def map_polars_to_gmw_constructions(df: pl.DataFrame, kvk: str) -> GMWConstruction:
    """
    Maps polars DataFrame to GMWConstruction objects.
    Groups by 'Putnaam' and creates one GMWConstruction per well with associated MonitoringTubes.
    """
    # Create monitoring tubes for this well
    monitoring_tubes = []
    for index, row in enumerate(df.iter_rows(named=True)):
        logger.info(f"Processing row {index + 1}: {row}")
        tube = create_monitoring_tube(
            row, tube_number=row["Filternummer"]
        )  # tube_number starts at 1
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
        owner=kvk,
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


def bulk_gmw_correction_request(kvk: str) -> None:
    """Use an excel to move multiple GMWs.

    Columns: gmw_id"""
    # Access your API key
    brostar = BROSTARConnection("HUhO9Jl2.rLXSyJq83wA9kQLT7wACNZbkZpK3eUug")  # BROSTAR API Key
    brostar.set_website(production=True)
    results = []
    r = brostar.get("gmw/gmws")
    r.raise_for_status()
    results += r.json().get("results", [])
    next = r.json().get("next")
    while next is not None:
        r = brostar.s.get(next)
        r.raise_for_status()
        results += r.json().get("results", [])
        next = r.json().get("next")

    print(results)
    df = pl.DataFrame(results, schema_overrides={"nitg_code": pl.String})
    df = df.filter(pl.col("nitg_code").is_not_null())
    df = df.select("uuid", "bro_id", "nitg_code")
    formatter = PayloadFormatter(brostar)

    for row in df.iter_rows(named=True):
        logger.info(row)
        bro_id = row.get("bro_id")

        construction = formatter.format_gmw_construction(bro_id)
        construction.object_id_accountable_party = (
            f"Correctie_{construction.nitg_code if construction.nitg_code else bro_id}"
        )
        construction.nitg_code = None

        metadata = UploadTaskMetadata(
            request_reference="20250718_Correctie_Tholen",
            delivery_accountable_party=str(kvk),
            quality_regime="IMBRO/A",
            bro_id=bro_id,
            correction_reason="inOnderzoek",
        )
        upload_task = UploadTask(
            bro_domain="GMW",
            project_number="981",
            registration_type="GMW_Construction",
            request_type="replace",
            sourcedocument_data=construction,
            metadata=metadata,
        )
        logger.info(upload_task.model_dump(mode="json", by_alias=True))
        _correct_gmw(brostar, upload_task)


def retry_upload_task() -> None:
    """Retry all upload tasks that are in PROCESSING state."""
    import re

    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    r = brostar.get("uploadtasks", params={"status": "FAILED"})
    for task in r.json().get("results", []):
        uuid = task["uuid"]
        logger.info(f"Retrying upload task {uuid}")

        if "mag niet voor de laatst geregistreerde gebeurtenis" in task["bro_errors"]:
            metadata = task["metadata"]
            metadata["correctionReason"] = "eigenCorrectie"
            retry_r = brostar.s.patch(
                url=f"{brostar.website}/uploadtasks/{uuid}/", json={"metadata": metadata}
            )
            retry_r.raise_for_status()

            metadata["request_type"] = "insert"
            retry_r = brostar.s.patch(
                url=f"{brostar.website}/uploadtasks/{uuid}/", json={"metadata": metadata}
            )
            retry_r.raise_for_status()

        if "moet liggen na of op de inrichtingsdatum" in task["bro_errors"]:
            # Extract all dates in YYYY-MM-DD format
            dates = re.findall(r"\d{4}-\d{2}-\d{2}", task["bro_errors"])

            if len(dates) >= 2:
                second_date = dates[1]  # The inrichtingsdatum
                sourcedocument_data = task["sourcedocument_data"]
                sourcedocument_data["eventDate"] = second_date

                retry_r = brostar.s.patch(
                    url=f"{brostar.website}/uploadtasks/{uuid}/",
                    json={"sourcedocument_data": sourcedocument_data},
                )
                retry_r.raise_for_status()

        if (
            "Dit brondocument is al eerder via het bronhouderportaal aangeleverd aan de BRO"
            in task["bro_errors"]
        ):
            retry_r = brostar.s.patch(
                url=f"{brostar.website}/uploadtasks/{uuid}/", json={"status": "COMPLETED"}
            )
            retry_r.raise_for_status()

            retry_r = brostar.s.patch(
                url=f"{brostar.website}/uploadtasks/{uuid}/", json={"progress": 100.0}
            )
            retry_r.raise_for_status()

            retry_r = brostar.s.patch(
                url=f"{brostar.website}/uploadtasks/{uuid}/", json={"log": ""}
            )
            retry_r.raise_for_status()
            continue

        # retry_r = brostar.s.patch(url=f"{brostar.website}/uploadtasks/{uuid}/", json={"status": "PENDING"})
        # retry_r.raise_for_status()


def bulk_gmw_construction_request(excel_file: str | Path, kvk: str) -> None:
    """Use an excel to create multiple GMWs."""
    # Access your API key
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    df = pl.read_excel(excel_file, has_header=True)
    putten = df.unique("Putnaam").to_series(0).to_list()

    for put in putten:
        construction = map_polars_to_gmw_constructions(df.filter(pl.col("Putnaam").eq(put)), kvk)
        ### Setup the payload
        metadata = UploadTaskMetadata(
            request_reference=f"{put}",
            delivery_accountable_party=kvk,
            quality_regime="IMBRO",  # Add to row?
        )

        ## Extract excel into GMW Construction
        sourcedocument_data = construction

        payload = UploadTask(
            bro_domain="GMW",
            project_number="1",
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


def total_events_delivered() -> int:
    """Retrieve the total number of events delivered."""
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    r = brostar.get(
        "uploadtasks", params={"status": "COMPLETED", "registration_type": "GLD_Addition"}
    )
    r.raise_for_status()

    total_count = 0
    next = r.json().get("next")
    bro_ids = []
    for result in r.json().get("results", []):
        bro_id = result.get("bro_id", None)
        events_count = result.get("sourcedocument_data", {}).get("timeValuePairsCount", 0)
        total_count += events_count
        bro_ids.append(bro_id)

    while next is not None:
        r = brostar.s.get(next)
        r.raise_for_status()
        for result in r.json().get("results", []):
            bro_id = result.get("bro_id", None)
            events_count = result.get("sourcedocument_data", {}).get("timeValuePairsCount", 0)
            total_count += events_count
            bro_ids.append(bro_id)

        next = r.json().get("next")

    bro_ids = list(set(bro_ids))  # Remove duplicates
    logger.info(f"Total unique GLD IDs: {len(bro_ids)}")

    return total_count


def deliver_gld_start_registration(
    internal_id: str,
    bro_id: str,
    tube_number: int,
    delivery_accountable_party: str,
    monitoring_nets: list[str],
    project_number: str,
) -> str | None:
    """Send a gld start registration request that corrects the dates."""

    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)
    sourcedocument_data = {
        "gmwBroId": bro_id,
        "tubeNumber": tube_number,
        "groundwaterMonitoringNets": eval(monitoring_nets),
        "objectIdAccountableParty": internal_id,
    }
    metadata = UploadTaskMetadata(
        request_reference="MeetnettenVitens-BROSTAR",
        delivery_accountable_party=delivery_accountable_party,
        quality_regime="IMBRO",
    )

    payload = UploadTask(
        bro_domain="GLD",
        project_number=str(project_number),
        registration_type="GLD_StartRegistration",
        request_type="registration",
        sourcedocument_data=sourcedocument_data,
        metadata=metadata,
    )
    payload = payload.model_dump(mode="json", by_alias=True)
    r = brostar.post_upload(payload)
    logger.info(r.json())
    r.raise_for_status()

    uuid: str = r.json()["uuid"]
    r = brostar.await_completed(uuid=uuid)

    return r.json().get("broId")


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


def create_bulk_gld(excel_file: str | Path) -> None:
    df = pl.read_excel(excel_file, has_header=True)
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    r = brostar.get("uploadtasks", params={"registration_type": "GLD_StartRegistration"})
    r.raise_for_status()

    results = []
    next = r.json().get("next")
    results += r.json().get("results", [])
    while next is not None:
        r = brostar.s.get(next)
        r.raise_for_status()
        results += r.json().get("results", [])
        next = r.json().get("next")

    df2 = pl.DataFrame(results)
    df2 = df2.with_columns(
        pl.col("sourcedocument_data").struct.field("objectIdAccountableParty").alias("business_id"),
    )
    df2 = df2.select(
        "bro_id",
        "business_id",
    )
    print(df2)

    df = df.join(df2, left_on="objectIdAccountableParty", right_on="business_id", how="left")
    print(df)

    bro_ids = []
    for _i, row in enumerate(df.iter_rows(named=True)):
        if row["groundwaterMonitoringNets"] is None:
            bro_ids.append(None)
            continue

        bro_id = deliver_gld_start_registration(
            internal_id=row["objectIdAccountableParty"],
            bro_id=row["gmwBroId"],
            tube_number=row["tubeNumber"],
            delivery_accountable_party=str(row["deliveryAccountableParty"]),
            monitoring_nets=row["groundwaterMonitoringNets"],
            project_number=row["projectNumber"],
        )
        bro_ids.append(bro_id)
        logger.info(bro_id)

    # Save to new Excel file with "v2" suffix
    new_filename = excel_file.replace(".xlsx", "_v2.xlsx")
    df = df.with_columns(pl.Series("broId", bro_ids))
    df.write_excel(new_filename)

    logger.info(f"Saved updated DataFrame to {new_filename}")


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


def gld_to_lizard(location_code: str, gld_id: str) -> None:
    lizard_api_key = os.getenv("LIZARD_API_KEY")
    lizard_s = requests.Session()
    lizard_s.headers = {
        "username": "__key__",
        "password": lizard_api_key,  # Lizard API Key
        "Content-Type": "application/json",
    }

    r = lizard_s.get(
        url="https://vitens.lizard.net/api/v4/locations/",
        params={"code": f"{location_code}"},
        timeout=15,
    )
    r.raise_for_status()
    if len(r.json()["results"]) == 0:
        logger.info(r.url)
        logger.info("No locations found.")
        return

    extra_metadata = r.json()["results"][0]["extra_metadata"]

    extra_metadata["bro"]["broid_gld_imbro"] = gld_id
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
