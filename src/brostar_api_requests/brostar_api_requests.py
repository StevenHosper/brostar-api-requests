import ast
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import polars as pl
import pytz
import requests
from dotenv import load_dotenv

from .connection import BROSTARConnection
from .formatter import PayloadFormatter
from .lizard_requests import setup_lizard_session
from .upload_models import (
    DeleteGLDAddition,
    GMWConstruction,
    MonitoringTube,
    TimeValuePair,
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
        r = brostar.get("uploadtasks", params={"status": "FAILED"})
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


def post_timeseries_events(
    timeseries_url: str, events_df: pl.DataFrame, session: requests.Session
) -> None:
    """Post timeseries events to lizard with adjusted flag"""
    logger.info(f"Posting timeseries to {f'{timeseries_url}events/'}.")
    logger.info(events_df)
    r = session.post(
        url=f"{timeseries_url}events/",
        json=events_df.to_dicts(),
        timeout=120,
    )
    logger.info(f"Response status code: {r.status_code} - {r.content}")
    r.raise_for_status()


def create_brostar_task(url: str, payload: dict, brostar_s: requests.Session) -> dict:
    r = brostar_s.post(url, json=payload, timeout=60)
    logger.info(r.url)
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


def get_observations(
    bro_id: str, observation_status: Literal["volledigBeoordeeld", "voorlopig", None, "niet"]
) -> pl.DataFrame | None:
    r = requests.get(
        f"https://publiek.broservices.nl/gm/gld/v1/objects/{bro_id}/observationsSummary"
    )

    results = r.json()
    if len(results) == 0:
        return None

    df = pl.DataFrame(results)
    if observation_status == "niet":
        return df

    df = df.filter(pl.col("observationStatus") == observation_status)
    logger.info(df)
    return df


def get_timeseries(location: dict, observation_code: str | None = None) -> list[dict]:
    ls = setup_lizard_session()
    params = {
        "limit": 50,
        "location__code": location["code"],
    }
    if observation_code is not None:
        params["observation_type__code"] = observation_code

    r = ls.get(
        url="https://vitens.lizard.net/api/v4/timeseries/",
        params=params,
    )
    r.raise_for_status()
    return r.json()["results"]


def post_timeseries(events: pl.DataFrame, timeserie_url: str):
    ls = setup_lizard_session()
    r = ls.post(
        url=f"{timeserie_url}events/",
        json=events.to_dicts(),
        timeout=120,
    )
    r.raise_for_status()
    logger.info("Succesfully posted events")
    logger.info(r)


def get_timeserie_events(
    timeserie_url: str, start_date: str | None = None, end_date: str | None = None
) -> pl.DataFrame:
    ls = setup_lizard_session()
    params = {
        "limit": 25000,
    }
    if start_date:
        params["start"] = start_date
    if end_date:
        params["end"] = end_date

    r = ls.get(
        url=f"{timeserie_url}events/",
        params=params,
    )
    r.raise_for_status()

    df = pl.from_dicts(
        r.json()["results"],
        schema=pl.Schema(
            {
                "time": pl.String,
                "value": pl.Float64,
                "flag": pl.Int64,
                "validation_code": pl.String,
                "comment": pl.String,
                "last_modified": pl.String,
                "detection_limit": pl.String,
            }
        ),
        strict=False,
    )

    while r.json()["next"]:
        r = ls.get(r.json()["next"])
        r.raise_for_status()

        df = df.vstack(
            pl.from_dicts(
                r.json()["results"],
                schema=pl.Schema(
                    {
                        "time": pl.String,
                        "value": pl.Float64,
                        "flag": pl.Int64,
                        "validation_code": pl.String,
                        "comment": pl.String,
                        "last_modified": pl.String,
                        "detection_limit": pl.String,
                    }
                ),
                strict=False,
            )
        )

    return df


def convert_dates(data: dict) -> dict:
    """
    Convert startDate and endDate from DD-MM-YYYY to YYYY-MM-DD format.
    Add 1 day to endDate.
    """
    # Parse input dates
    start = datetime.strptime(data["startDate"], "%d-%m-%Y")
    end = datetime.strptime(data["endDate"], "%d-%m-%Y") + timedelta(days=1)

    # Update dictionary with new format
    data["startDate"] = start.strftime("%Y-%m-%d")
    data["endDate"] = end.strftime("%Y-%m-%d")

    return data


def retrieve_reset_events(timeserie_url: str):
    events = get_timeserie_events(timeserie_url)
    events = events.with_columns(pl.lit("").alias("validation_code"))
    if events.is_empty():
        logger.info("No events found for timeframe")
    else:
        logger.info(events.head())
        post_timeseries(events, timeserie_url)
        logger.info(f"Found and adjusted {events.height} events")


def retrieve_and_adjust_events(
    timeserie_url: str, start_date: str, end_date: str, validation_code: str = "V"
):
    events = get_timeserie_events(timeserie_url, start_date, end_date)
    events = events.with_columns(pl.lit(validation_code).alias("validation_code"))
    if events.is_empty():
        logger.info("No events found for timeframe")
    else:
        logger.info(events.head())
        post_timeseries(events, timeserie_url)
        logger.info(f"Found and adjusted {events.height} events")


def mark_events_in_observation_periods(
    events: pl.DataFrame, observations: pl.DataFrame
) -> pl.DataFrame:
    """
    Mark events with validation_code='V' if they fall within any observation period.
    Events are marked if: startDate <= event_time < endDate (endDate already has +1 day)
    """
    # Ensure validation_code column exists
    if "validation_code" not in events.columns:
        events = events.with_columns(pl.lit("").alias("validation_code"))

    # Start with current validation codes
    result = events.clone()

    # For each observation period, mark events that fall within it
    for obs in observations.iter_rows(named=True):
        start = obs["startDate"]  # Already in YYYY-MM-DD format
        end = obs["endDate"]  # Already in YYYY-MM-DD format with +1 day

        # Mark events where startDate <= time < endDate with 'V'
        result = result.with_columns(
            pl.when((pl.col("time") >= start) & (pl.col("time") < end))
            .then(pl.lit("V"))
            .otherwise(pl.col("validation_code"))
            .alias("validation_code")
        )

    return result


def adjust_validation_code_lizard_based_on_bro(
    organisation_uuid: str,
    location_code: str,
    observation_status: Literal[
        "volledigBeoordeeld", "voorlopig", None, "niet"
    ] = "volledigBeoordeeld",
    observation_code: str = "WNS9040",
) -> None:
    """
    Observation status = None -> Controle reeks
    Observation status = 'volledigBeoordeeld' -> Volledig beoordeeld reguliere reeks
    Observation status = 'voorlopig' -> Voorlopig reguliere reeks
    """
    ls = setup_lizard_session()

    r = ls.get(
        url="https://vitens.lizard.net/api/v4/locations/",
        params={
            "object__type": "filter",
            "code__startswith": location_code,
            "organisation__uuid": organisation_uuid,
            "limit": 25000,
        },
    )

    for location in r.json().get("results", []):
        logger.info(f"Processing location: {location['code']}")
        bro_id_imbroa = location["extra_metadata"].get("bro", {}).get("gldIdImbroA", None)
        bro_id_imbro = location["extra_metadata"].get("bro", {}).get("gldIdImbro", None)

        logger.info(
            f"Processing location {location['code']} with IMBRO/A ID {bro_id_imbroa} and IMBRO ID {bro_id_imbro}"
        )
        if bro_id_imbro not in ["", None]:
            observations_df_imbro = get_observations(bro_id_imbro, observation_status)
            logger.info(f"Found observations_df_imbro: {observations_df_imbro}")
            observations_df = observations_df_imbro

        if bro_id_imbroa not in ["", None]:
            observations_df_imbroa = get_observations(bro_id_imbroa, observation_status)
            logger.info(f"Found observations_df_imbroa: {observations_df_imbroa}")
            if observations_df is None or observations_df.is_empty():
                observations_df = observations_df_imbroa
            elif observations_df_imbroa is not None and not observations_df_imbroa.is_empty():
                observations_df = pl.concat([observations_df, observations_df_imbroa])

        # If no observations, nothing to do
        if observations_df is None or observations_df.is_empty():
            logger.info(f"No observations found for location: {location['code']}")
            return

        # Convert dates: DD-MM-YYYY -> YYYY-MM-DD and add 1 day to endDate
        observations_df = observations_df.with_columns(
            [
                pl.col("startDate")
                .str.to_date("%d-%m-%Y")
                .dt.strftime("%Y-%m-%d")
                .alias("startDate"),
                pl.col("endDate")
                .str.to_date("%d-%m-%Y")
                .dt.offset_by("1d")
                .dt.strftime("%Y-%m-%d")
                .alias("endDate"),
            ]
        )

        # Get the overall date range
        min_start = observations_df["startDate"].min()
        max_end = observations_df["endDate"].max()

        timeseries = get_timeseries(location, observation_code=observation_code)
        for timeserie in timeseries:
            # Get ALL events in the date range with ONE request
            all_events = get_timeserie_events(timeserie["url"], min_start, max_end)

            if all_events.is_empty():
                logger.info(f"No events found for timeserie in date range {min_start} to {max_end}")
                continue

            # Mark events as 'V' if they fall within any observation period
            all_events = mark_events_in_observation_periods(all_events, observations_df)

            # Post ALL updated events in ONE request
            if not all_events.is_empty():
                post_timeseries(all_events, timeserie["url"])
                logger.info(f"Posted {len(all_events)} events for timeserie")


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
        horizontal_positioning_method=first_row.get("Methode Coordinatenbepaling", ""),
        local_vertical_reference_point="NAP",  # Always NAP as specified
        offset=0.0,  # No mapping available - needs default
        vertical_datum="NAP",  # Always NAP as specified
        ground_level_position=first_row.get("Maaiveldpositie (m+NAP)"),
        ground_level_positioning_method=first_row.get("Methode Maaiveldpositiebepaling", ""),
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
        variable_diameter=row.get("Variabele diameter"),
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

    logger.info(results)
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
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    r = brostar.get("uploadtasks", params={"status": "PROCESSING"})
    for task in r.json().get("results", []):
        uuid = task["uuid"]
        logger.info(f"Retrying upload task {uuid}")

        retry_r = brostar.s.patch(
            url=f"{brostar.website}/uploadtasks/{uuid}/", json={"status": "PENDING"}, timeout=30
        )
        logger.info(retry_r)
        logger.info(retry_r.content)
        retry_r.raise_for_status()


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
        logger.info(payload)
        r = brostar.post_upload(payload=payload, is_json=True)
        logger.info(r.json())
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
    quality_regime: Literal["IMBRO", "IMBRO/A"],
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
        request_reference=f"{internal_id}-{quality_regime}",
        delivery_accountable_party=delivery_accountable_party,
        quality_regime=quality_regime,
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


def deliver_frd_start_registration(
    internal_id: str,
    bro_id: str,
    tube_number: int,
    delivery_accountable_party: str,
    monitoring_nets: list[str],
    project_number: str,
    quality_regime: Literal["IMBRO", "IMBRO/A"],
) -> str | None:
    """Send a gld start registration request that corrects the dates."""

    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)
    sourcedocument_data = {
        "gmwBroId": bro_id,
        "tubeNumber": tube_number,
        "groundwaterMonitoringNets": monitoring_nets,
        "objectIdAccountableParty": internal_id,
    }
    metadata = UploadTaskMetadata(
        request_reference=f"{internal_id}-{quality_regime}",
        delivery_accountable_party=delivery_accountable_party,
        quality_regime=quality_regime,
    )

    payload = UploadTask(
        bro_domain="FRD",
        project_number=str(project_number),
        registration_type="FRD_StartRegistration",
        request_type="registration",
        sourcedocument_data=sourcedocument_data,
        metadata=metadata,
    )
    payload = payload.model_dump(mode="json", by_alias=True)
    print(payload)
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


def is_gld_id(bro_id: str | None) -> bool:
    """Check if a string is a valid BRO ID. GLD000000091284"""
    if bro_id is None:
        logger.warning("BRO ID is None.")
        return False
    if bro_id.startswith("GLD") and len(bro_id) == 15:
        logger.info(f"BRO ID is correct: {bro_id}.")
        return True

    logger.warning(f"BRO ID is incorrect: {bro_id}.")
    return False


def get_pdok_attributes(bro_id: str, attributes: list[str]) -> dict[str, str]:
    """Retrieve the specified attributes from PDOK for a given BRO ID."""
    r = requests.get(
        f"https://api.pdok.nl/bzk/bro-gminsamenhang-karakteristieken/ogc/v1/collections/gm_gld/items?f=json&bro_id={bro_id}",
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    attributes_data = data.get("features", [])[0].get("properties", {})
    if not attributes_data:
        raise ValueError(f"Attributes not found for BRO ID: {bro_id}")

    logger.info(f"Attributes for {bro_id} are {attributes_data}.")
    return {attr: attributes_data.get(attr) for attr in attributes}


def get_gmw_id_tube_nr(gld_id: str, brostar: BROSTARConnection):
    r = brostar.get("gld/glds", params={"bro_id": gld_id})
    r.raise_for_status()
    results = r.json().get("results", [])
    if len(results) == 0:
        logger.warning(f"No GLD found for GLD ID {gld_id}.")
        return None, None

    gmw_bro_id = results[0].get("gmw_bro_id", None)
    tube_number = results[0].get("tube_number", 1)
    return gmw_bro_id, tube_number


class GLDCorrecter:
    def __init__(self, bro_id: str | None = None) -> None:
        brostar_api_key = os.getenv("BROSTAR_API_KEY")
        brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
        brostar.set_website(production=True)
        self.brostar = brostar
        if bro_id is not None:
            self.set_bro_id(bro_id)

    def set_bro_id(self, bro_id: str) -> None:
        if not is_gld_id(bro_id):
            raise ValueError(f"Invalid GLD BRO ID: {bro_id}")

        self.bro_id = bro_id
        attributes = get_pdok_attributes(bro_id, ["delivery_accountable_party", "quality_regime"])
        self.delivery_accountable_party = attributes.get("delivery_accountable_party", "")
        self.quality_regime = attributes.get("quality_regime", "")

    def set_project_number(self, project_number: str) -> None:
        self.project_number = project_number

    def delete_observation(self, observation_id: str) -> None:
        if self.bro_id is None:
            raise ValueError("BRO ID is not set.")

        r = requests.get(
            f"https://publiek.broservices.nl/gm/gld/v1/objects/{self.bro_id}/observationsSummary",
            timeout=30,
        )
        observations = pl.DataFrame(r.json())
        observation = observations.filter(pl.col("observationId") == observation_id)

        if len(observation) == 0:
            logger.info(f"No observation found for {self.bro_id} with ID {observation_id}.")
            return

        row = observation.row(0, named=True)
        logger.info(
            f"Deleting observation {row['observationId']} - {row['startDate']} - {row['endDate']}"
        )

        row["endDate"] = datetime.strptime(row["endDate"], "%d-%m-%Y").date().isoformat()
        row["startDate"] = (
            (datetime.strptime(row["startDate"], "%d-%m-%Y") + timedelta(1)).date().isoformat()
        )

        source_doc_data = DeleteGLDAddition(
            observation_id=row["observationId"],
            observation_process_id=row["observationProcessId"],
            observation_status=row["observationStatus"],
            begin_position=row["startDate"],
            end_position=row["endDate"],
            observation_type=row["observationType"],
            time_value_pairs=[
                TimeValuePair(time="1900-01-01T00:00:00Z", value=0)
            ],  # Empty time value pairs for deletion
        )
        source_doc_data.result_time = datetime.now(tz=AMS_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_doc_data.date = datetime.now(tz=AMS_TZ).strftime("%Y-%m-%d")

        payload = UploadTask(
            bro_domain="GLD",
            project_number=self.project_number,
            registration_type="GLD_Addition",
            request_type="delete",
            metadata=UploadTaskMetadata(
                request_reference=f"Delete_{self.bro_id}_{row['observationId']}_{datetime.now(tz=AMS_TZ).strftime('%Y-%m-%dT%H:%M:%SZ')}",
                delivery_accountable_party=self.delivery_accountable_party,  # Adjust as needed
                quality_regime=self.quality_regime,
                bro_id=self.bro_id,
                correction_reason="eigenCorrectie",
            ),
            sourcedocument_data=source_doc_data,
        )

        r = self.brostar.post_upload(
            payload=payload.model_dump(mode="json", by_alias=True), is_json=True
        )
        r.raise_for_status()

        uuid: str = r.json()["uuid"]
        self.brostar.await_completed(uuid=uuid)
        logger.info(
            f"Should correct gld: {self.bro_id} from start {row['startDate']} to end {row['endDate']}"
        )
        # Adjust lizard events

        gmw_id, tube_number = get_gmw_id_tube_nr(self.bro_id, self.brostar)
        location_code = f"{gmw_id}-{int(tube_number):03d}"
        ls = setup_lizard_session()
        r = ls.get(
            url=f"https://vitens.lizard.net/api/v4/timeseries/?location__code={location_code}&observation_type__code=WNS9040.hand"
        )
        timeserie_url = r.json()["results"][0]["url"]
        retrieve_and_adjust_events(
            timeserie_url=timeserie_url,
            start_date=row["startDate"],
            end_date=row["endDate"],
            validation_code="",
        )

    def delete_observations(
        self, start_date: datetime | None = None, lower_then: bool = False
    ) -> None:
        if self.bro_id is None:
            raise ValueError("BRO ID is not set.")

        r = requests.get(
            f"https://publiek.broservices.nl/gm/gld/v1/objects/{self.bro_id}/observationsSummary",
            timeout=30,
        )
        if len(r.json()) == 0:
            logger.info(f"No observations found for {self.bro_id}.")
            return

        observations = pl.DataFrame(r.json())
        observations = observations.with_columns(
            pl.col("startDate").str.strptime(pl.Datetime, format="%d-%m-%Y").alias("startDate"),
            pl.col("endDate").str.strptime(pl.Datetime, format="%d-%m-%Y").alias("endDate"),
        )
        if start_date is not None and lower_then:
            observations = observations.filter(pl.col("startDate") < start_date)
        elif start_date is not None and not lower_then:
            observations = observations.filter(pl.col("startDate") >= start_date)

        if len(observations) == 0:
            logger.info(f"No observations found for {self.bro_id} with startDate {start_date}.")
            return

        logger.info(
            f"Found {len(observations)} observations for {self.bro_id} with startDate {start_date}."
        )

        for row in observations.iter_rows(named=True):
            logger.info(f"{row['observationId']} - {row['startDate']}")

            source_doc_data = DeleteGLDAddition(
                observation_id=row["observationId"],
                observation_process_id=row["observationProcessId"],
                observation_status=row["observationStatus"],
                begin_position=row["startDate"],
                end_position=row["endDate"],
                observation_type=row["observationType"],
                time_value_pairs=[
                    TimeValuePair(time="1900-01-01T00:00:00Z", value=0)
                ],  # Empty time value pairs for deletion
            )
            source_doc_data.result_time = datetime.now(tz=AMS_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")
            source_doc_data.date = datetime.now(tz=AMS_TZ).strftime("%Y-%m-%d")

            payload = UploadTask(
                bro_domain="GLD",
                project_number=self.project_number,
                registration_type="GLD_Addition",
                request_type="delete",
                metadata=UploadTaskMetadata(
                    request_reference=f"Delete_{self.bro_id}_{row['observationId']}_{datetime.now(tz=AMS_TZ).strftime('%Y-%m-%dT%H:%M:%SZ')}",
                    delivery_accountable_party=self.delivery_accountable_party,  # Adjust as needed
                    quality_regime=self.quality_regime,
                    bro_id=self.bro_id,
                    correction_reason="eigenCorrectie",
                ),
                sourcedocument_data=source_doc_data,
            )

            r = self.brostar.post_upload(
                payload=payload.model_dump(mode="json", by_alias=True), is_json=True
            )
            print(r.content)
            print(r.status_code)
            r.raise_for_status()

            uuid: str = r.json()["uuid"]
            self.brostar.await_completed(uuid=uuid)


def correct_gld_dossier_for_observation_request(
    current_id: str,
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
        logger.info(result)
        r = brostar.post_upload(payload=result, is_json=True)
        r.raise_for_status()

        uuid: str = r.json()["uuid"]
        brostar.await_completed(uuid=uuid)


def convert_to_list(s):
    return ast.literal_eval(s)


def handle_errors(brostar, task):
    import re

    errors = eval(task.get("bro_errors", "[]"))
    print("BRO errors:", errors)

    if not errors:
        return

    # Use the error message containing the difference
    error_text = None
    for e in errors:
        if "het verschil in Inkorten.monitoringbuis.positie bovenkant buis" in e:
            error_text = e
            break

    if not error_text:
        print("No matching error found for automatic correction.")
        return

    print(error_text)
    # Regex to extract tubeTopPosition and plainTubePartLength
    tube_top_match = re.search(
        r"\(Shortening\.monitoringTube\.tubeTopPosition\) = (-?[0-9.]+)", error_text
    )
    plain_tube_match = re.search(
        r"\(Shortening\.monitoringTube\.monitoringTube\.plainTubePartLength\) = (-?[0-9.]+)",
        error_text,
    )

    if tube_top_match and plain_tube_match:
        tube_top = float(tube_top_match.group(1))
        plain_tube = float(plain_tube_match.group(1))
        print("tubeTopPosition:", tube_top)
        print("plainTubePartLength:", plain_tube)

        difference = tube_top - plain_tube
        print("Difference (tubeTopPosition - plainTubePartLength):", difference)

        # Calculate correction
        correction = round(difference, 3)
        print(f"Adjusting plain tube part length by {correction}")

        # Adjust in sourcedocument_data
        sourcedocument_data = task.get("sourcedocument_data", {})
        if (
            "monitoringTubes" in sourcedocument_data
            and len(sourcedocument_data["monitoringTubes"]) > 0
        ):
            sourcedocument_data["monitoringTubes"][0]["plainTubePartLength"] -= correction

            # Patch the task
            patch_data = {
                "sourcedocument_data": sourcedocument_data,
                "status": "PENDING",
            }
            patch_resp = brostar.s.patch(task["url"], json=patch_data)
            print("Patch response:", patch_resp.status_code, patch_resp.text)
        else:
            print("No monitoringTubes found to adjust.")
    else:
        print("Could not parse error for automatic correction.")


def check_status_processing_upload_tasks() -> None:
    """Delete all upload tasks that are in PROCESSING state."""
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)
    # https://www.brostar.nl/api/uploadtasks/?created__lte=2025-07-01&status=COMPLETED&registration_type=GLD_Addition
    r = brostar.get(
        "uploadtasks",
        params={
            "status": "COMPLETED",
            "created__lte": "2025-07-01",
            "registration_type": "GLD_Addition",
            "request_type": "registration",
        },
    )
    next = r.url
    info = []
    while next is not None:
        r = brostar.s.get(next)
        print(r.url)
        print(r.content)
        r.raise_for_status()
        tasks = r.json()["results"]
        for task in tasks:
            if (
                task["sourcedocument_data"].get("observationType", "reguliereMeting")
                == "controlemeting"
            ):
                continue

            if task["sourcedocument_data"].get("timeValuePairsCount", 0) < 10:
                continue

            info.append(
                {
                    "broId": task["bro_id"],
                    "observationId": task["sourcedocument_data"].get("observationId", ""),
                    "tvpCount": task["sourcedocument_data"].get("timeValuePairsCount", 0),
                }
            )
            # res = brostar.s.patch(task["url"], json={"status": "PENDING"}, timeout=30)
            # res = brostar.s.post(task["url"] + "check_status/", json={}, timeout=30)
            # print(res.url)
            # print(res.status_code, res.content)

        next = r.json().get("next")

    df = pl.DataFrame(info)
    df.write_csv("20251215_Rotterdam.csv")


def download_xml_upload_tasks() -> None:
    """Download all upload tasks"""
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    pairs = [
        ("GLD000000098258", "GLD000000038928"),
        ("GLD000000098287", "GLD000000038965"),
        ("GLD000000098259", "GLD000000038959"),
        ("GLD000000098260", "GLD000000038967"),
        ("GLD000000098288", "GLD000000038947"),
        ("GLD000000098289", "GLD000000038939"),
        ("GLD000000098290", "GLD000000038929"),
        ("GLD000000098291", "GLD000000038928"),
        ("GLD000000098292", "GLD000000038963"),
        ("GLD000000098261", "GLD000000038932"),
        ("GLD000000098293", "GLD000000038953"),
        ("GLD000000098294", "GLD000000038968"),
        ("GLD000000098265", "GLD000000038945"),
        ("GLD000000098297", "GLD000000038962"),
        ("GLD000000098326", "GLD000000038960"),
        ("GLD000000098302", "GLD000000038941"),
        ("GLD000000098328", "GLD000000038934"),
        ("GLD000000098330", "GLD000000038946"),
        ("GLD000000098303", "GLD000000038951"),
        ("GLD000000098304", "GLD000000038944"),
    ]
    for current_id, new_id in pairs:
        r = brostar.get(
            "uploadtasks",
            params={
                "status": "COMPLETED",
                "bro_id": current_id,
                "registration_type": "GLD_Addition",
            },
        )
        r.raise_for_status()
        tasks = r.json()["results"]
        for task in tasks:
            logger.info(task["sourcedocument_data"])
            if task["sourcedocument_data"]["observationType"] == "controlemeting":
                metadata = task["metadata"]
                metadata["broId"] = new_id
                r = brostar.s.patch(task["url"], json={"metadata": metadata})
                logger.info(r.status_code)

                r = brostar.s.get(task["bro_delivery_url"] + "read_xml/", timeout=30)
                # returns full xml content, should save this to file.
                with open(f"{current_id}_to_{new_id}.xml", "wb") as f:
                    f.write(r.content)


def fix_upload_tasks() -> None:
    """Delete all upload tasks that are in PROCESSING state."""
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    r = brostar.get("uploadtasks", params={"status": "FAILED", "registration_type": "GLD_Addition"})
    next = r.url
    total_tasks = []
    while next is not None:
        r = brostar.s.get(next)
        r.raise_for_status()
        tasks = r.json()["results"]
        total_tasks += tasks

        next = r.json().get("next")

    df = pl.DataFrame(total_tasks)
    df = df.with_columns(
        pl.col("metadata").struct.field("requestReference").alias("reference"),
    )

    df = df.sort("reference")

    logger.info(df.head())
    old_reference = ""
    for task in df.iter_rows(named=True):
        logger.info(f"handling task {task['uuid']} with reference {task['reference']}")
        current_reference = task["reference"]
        if current_reference == old_reference:
            logger.info(
                f"deleting duplicate task {task['uuid']} with reference {current_reference}"
            )
            brostar.s.delete(url=f"{brostar.website}/uploadtasks/{task['uuid']}/", timeout=30)
            continue

        r = brostar.s.get(f"{brostar.website}/uploadtasks/{task['uuid']}/", timeout=30)
        r.raise_for_status()

        sourcedocument_data = r.json()["sourcedocument_data"]
        sourcedocument_data.pop("airPressureCompensationType")

        brostar.s.patch(
            url=f"{brostar.website}/uploadtasks/{task['uuid']}/",
            json={
                "status": "PENDING",
                "progress": 0,
                "log": "",
                "sourcedocument_data": sourcedocument_data,
                "bro_errors": "",
            },
        )

        old_reference = current_reference


def correct_bulk_gld(csv_file: str | Path) -> None:
    df = pl.read_csv(csv_file, has_header=True, separator=";")

    total = df.height
    logger.info(f"Total rows to process: {total}")
    skip_count = 0
    for i, row in enumerate(df.iter_rows(named=True)):
        logger.info(f"Processing row {i + 1}/{total}: {row}")
        if i + 1 < 54:
            logger.info(f"Skipping row {i + 1} as per condition.")
            continue

        r = requests.get(
            f"https://publiek.broservices.nl/gm/gld/v1/objects/{row['gld']}/observationsSummary",
            timeout=30,
        )
        if len(r.json()) == 0:
            skip_count += 1
            logger.info(f"No observations found for {row['tube']}. Skipping.")
            continue

        correct_gld_dossier_for_observation_request(
            current_id=row["gld"],
        )
        logger.info(f"Completed processing row {i + 1}/{total}")

    logger.info(f"Skipped {skip_count} rows due to no observations found.")


def create_bulk_gld(excel_file: str | Path) -> None:
    df = pl.read_excel(excel_file, has_header=True)
    df.drop_in_place("bro_id")
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
    logger.info(df2)

    df = df.join(df2, left_on="objectIdAccountableParty", right_on="business_id", how="left")
    logger.info(df)

    bro_ids = []
    count = 0
    for _i, row in enumerate(df.iter_rows(named=True)):
        if row["groundwaterMonitoringNets"] is None:
            logger.info(f"Skipping row {_i + 1} due to missing groundwaterMonitoringNets.")
            bro_ids.append(None)
            continue

        if row["bro_id"] is not None and row["bro_id"] != "":
            bro_ids.append(row["bro_id"])
            logger.info(
                f"Skipping existing BRO ID for {row['objectIdAccountableParty']}: {row['bro_id']}"
            )
            continue

        count += 1
        logger.info(f"Processing row {_i + 1}: {row}")
        bro_id = None
        # bro_id = deliver_gld_start_registration(
        #     internal_id=row["objectIdAccountableParty"],
        #     bro_id=row["gmwBroId"],
        #     tube_number=row["tubeNumber"],
        #     delivery_accountable_party=str(row["deliveryAccountableParty"]),
        #     monitoring_nets=row["groundwaterMonitoringNets"],
        #     project_number=row["projectNumber"],
        # )
        bro_ids.append(bro_id)
        logger.info(bro_id)

    # Save to new Excel file with "v2" suffix
    new_filename = excel_file.replace("_v2.xlsx", "_v3.xlsx")
    df = df.with_columns(pl.Series("broId", bro_ids))
    df.write_excel(new_filename)

    logger.info(f"Saved updated DataFrame to {new_filename}")
    logger.info(f"Total new BRO IDs created: {count}")


def process_result(result: dict) -> None:
    lizard_api_key = os.getenv("LIZARD_API_KEY")
    lizard_s = requests.Session()
    lizard_s.headers = {
        "username": "__key__",
        "password": lizard_api_key,  # Lizard API Key
        "Content-Type": "application/json",
    }

    r = lizard_s.get(
        url="https://vitens.lizard.net/api/v4/locations/",
        params={
            "code": f"{result['sourcedocument_data']['objectIdAccountableParty']}",
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
        extra_metadata["bro"]["broid_gld_imbro"] = result["bro_id"]
        logger.info(extra_metadata["bro"])
    else:
        extra_metadata["bro"]["broid_gld_imbroa"] = result["bro_id"]
        logger.info(extra_metadata["bro"])

    r = lizard_s.patch(
        url=r.json()["results"][0]["url"], json={"extra_metadata": extra_metadata}, timeout=15
    )
    r.raise_for_status()
    logger.info(r.json())
    logger.info("\n\n")


def gld_to_lizard(location_code: str, gld_id_imbro: str, gld_id_imbroa: str) -> None:
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

    if extra_metadata["bro"]["broid_gld_imbro"] in [None, "", "NULL"]:
        extra_metadata["bro"]["broid_gld_imbro"] = gld_id_imbro if gld_id_imbro != "NULL" else None

    if extra_metadata["bro"]["broid_gld_imbroa"] in [None, "", "NULL"]:
        extra_metadata["bro"]["broid_gld_imbroa"] = (
            gld_id_imbroa if gld_id_imbroa != "NULL" else None
        )

    logger.info(extra_metadata["bro"])

    r = lizard_s.patch(
        url=r.json()["results"][0]["url"], json={"extra_metadata": extra_metadata}, timeout=15
    )
    r.raise_for_status()
    logger.info(r.json())
    logger.info("\n\n")


def ingest_gld_ids_into_lizard():
    """Retrieve all uploadtasks / registrations and ingest the information into Lizard."""
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    r = brostar.get(
        "uploadtasks",
        params={
            "registration_type": "GLD_StartRegistration",
            "status": "COMPLETED",
            "project_number": "1366",
        },
    )
    print(r.json())
    while r.json()["next"] is not None:
        for result in r.json()["results"]:
            logger.info(f"Processing {result}")

            # Get the bro_id from the registration and update Lizard
            process_result(result)

        r = brostar.s.get(url=r.json()["next"], timeout=15)
