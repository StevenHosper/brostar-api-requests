import datetime
import logging

import polars as pl
import requests

from brostar_api_requests.brostar_api_requests import (
    check_status_processing_upload_tasks,
    deliver_frd_start_registration,
    setup_lizard_session,
)

logger = logging.getLogger(__name__)


def to_datetime(date_str: str) -> datetime.datetime:
    """Convert a date string in the format 'YYYY-MM-DD' to 'YYYY-MM-DDT00:00:00Z'."""
    return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")


def create_frd_vitens():
    file_path = r"C:\Users\steven.hosper\Downloads\20251105_frd_data_vitens.csv"
    df = pl.read_csv(file_path, separator=",", has_header=True, truncate_ragged_lines=True)

    for row in df.iter_rows(named=True):
        deliver_frd_start_registration(
            internal_id=row["intern_id"],
            bro_id=row["broid_gmw"],
            tube_number=row["filternummer"],
            delivery_accountable_party="05069581",
            monitoring_nets=[],
            project_number="6708",
            quality_regime="IMBRO",
        )


def rotterdam_count():
    import os

    from src.brostar_api_requests.brostar_api_requests import BROSTARConnection

    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    next = True
    count = 0
    tasks = []
    while next is not None:
        print(f"Fetching page {count}")
        count += 1
        r = brostar.get(
            "uploadtasks",
            params={"status": "COMPLETED", "registration_type": "GLD_Addition", "page": count},
        )

        tasks += r.json().get("results", [])
        next = r.json().get("next", None)

    print(f"Total upload tasks: {len(tasks)}")

    total_measurements = 0
    for task in tasks:
        total_measurements += task["sourcedocument_data"].get("timeValuePairsCount", 0)

    print(f"Total measurements: {total_measurements}")


def adjust_lizard():
    s = setup_lizard_session()
    df = pl.read_csv(
        "lizard_check_rotterdam.csv", separator=";", has_header=True, truncate_ragged_lines=True
    )
    check = []

    print(df.sort("gld_imbro_match"))
    df = df.filter(
        pl.col("gld_imbroa_match").eq(False),
    ).sort("location_code")
    print(df)
    for row in df.iter_rows(named=True):
        code = row["location_code"]
        # gld_imbro_bro_id = row['broid_gld_imbro']
        gld_imbroa_bro_id = row["broid_gld_imbroa"]
        print(f"Processing location {code}...")
        lizard_location = (
            s.get(f"https://rotterdam.lizard.net/api/v4/locations/?code={code}")
            .json()
            .get("results", [])[0]
        )

        extra_metadata = lizard_location.get("extra_metadata", {})
        if "broid_gld_imbro" in extra_metadata["bro"]:
            extra_metadata["bro"].pop("broid_gld_imbro")

        if "broid_gld_imbroa" in extra_metadata["bro"]:
            extra_metadata["bro"].pop("broid_gld_imbroa")

        extra_metadata["bro"]["gldIdImbroA"] = gld_imbroa_bro_id

        s.patch(lizard_location["url"], json={"extra_metadata": extra_metadata})

    df_check = pl.DataFrame(check)
    print(df_check)


def get_gld_ids_bro(gmw_id: str, tube_number: int):
    r = requests.get(
        f"https://publiek.broservices.nl/gm/v1/gmw-relations/{gmw_id}?inclMetadata=true",
        timeout=30,
    )
    logger.warning(f"Checking GLD IDs from the BRO: {r.url}.")
    r.raise_for_status()
    monitoring_tubes = r.json().get("monitoringTubeReferences", [])
    df = None
    logger.info(monitoring_tubes)
    for tube in monitoring_tubes:
        logger.info(f"{tube['tubeNumber']} - {tube_number}")
        if int(tube["tubeNumber"]) == int(tube_number):
            df = pl.DataFrame(tube["gldReferences"])
            logger.info(df.head())

    if df is None:
        logger.warning(f"No GLD IDs found for {gmw_id}-{tube_number}.")
        return None, None

    if df.is_empty():
        logger.warning(f"No GLD IDs found for {gmw_id}-{tube_number}.")
        return None, None

    df = df.with_columns(
        pl.col("characteristics").struct.field("qualityRegime").alias("qualityRegime"),
        pl.col("characteristics").struct.field("researchFirstDate").alias("researchFirstDate"),
        pl.col("characteristics").struct.field("researchLastDate").alias("researchLastDate"),
    )
    imbro_tubes = df.filter(pl.col("qualityRegime").eq("IMBRO")).sort("broId", nulls_last=True)
    imbroa_tubes = df.filter(pl.col("qualityRegime").eq("IMBRO/A")).sort("broId", nulls_last=True)

    if imbro_tubes.is_empty():
        imbro_id = None
    else:
        imbro_id = (
            str(imbro_tubes["broId"].to_list())
            if imbro_tubes.height > 1
            else str(imbro_tubes["broId"][0])
        )

    if imbroa_tubes.is_empty():
        imbroa_id = None
    else:
        imbroa_id = (
            str(imbroa_tubes["broId"].to_list())
            if imbroa_tubes.height > 1
            else str(imbroa_tubes["broId"][0])
        )

    logger.info(f"Found IMBRO: {imbro_id} and IMBRO/A: {imbroa_id}.")
    return imbro_id, imbroa_id


def check_lizard():
    s = setup_lizard_session()
    df = pl.read_csv(
        "lizard_adjusted2.csv", separator=",", has_header=True, truncate_ragged_lines=True
    )
    check = []

    for row in df.iter_rows(named=True):
        print(row)
        code = row["location_code"]
        tube_number = int(code[-3:])
        gmw_id = code.split("-")[0]
        gld_imbro_bro_id = row["lizard_broid_gld_imbro"]
        gld_imbroa_bro_id = row["lizard_broid_gld_imbroa"]
        lizard_location = (
            s.get(f"https://rotterdam.lizard.net/api/v4/locations/?code={code}")
            .json()
            .get("results", [])[0]
        )

        imbro_id, imbroa_id = get_gld_ids_bro(gmw_id, tube_number)

        extra_metadata = lizard_location.get("extra_metadata", {})
        gld_lizard_imbro_bro_id = extra_metadata.get("bro", {}).get("gldIdImbro", None)
        gld_lizard_imbroa_bro_id = extra_metadata.get("bro", {}).get("gldIdImbroA", None)

        if (
            gld_imbro_bro_id != gld_lizard_imbro_bro_id
            or gld_imbroa_bro_id != gld_lizard_imbroa_bro_id
        ):
            check.append(
                {
                    "location_code": code,
                    "expected_broid_gld_imbro": gld_imbro_bro_id,
                    "lizard_broid_gld_imbro": gld_lizard_imbro_bro_id,
                    "bro_imbro_id": imbro_id,
                    "match_imbro": gld_lizard_imbro_bro_id == imbro_id,
                    "expected_broid_gld_imbroa": gld_imbroa_bro_id,
                    "lizard_broid_gld_imbroa": gld_lizard_imbroa_bro_id,
                    "bro_imbroa_id": imbroa_id,
                    "match_imbroa": gld_lizard_imbroa_bro_id == imbroa_id,
                }
            )

    df_check = pl.DataFrame(
        check,
        schema=[
            ("location_code", pl.Utf8),
            ("expected_broid_gld_imbro", pl.Utf8),
            ("lizard_broid_gld_imbro", pl.Utf8),
            ("bro_imbro_id", pl.Utf8),
            ("match_imbro", pl.Boolean),
            ("expected_broid_gld_imbroa", pl.Utf8),
            ("lizard_broid_gld_imbroa", pl.Utf8),
            ("bro_imbroa_id", pl.Utf8),
            ("match_imbroa", pl.Boolean),
        ],
    )
    print(df_check)
    df_check.write_csv("lizard_adjusted3.csv")


def empty_and_redeliver():
    import os

    from src.brostar_api_requests.brostar_api_requests import BROSTARConnection, GLDCorrecter

    ls = setup_lizard_session()

    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    df = pl.read_csv(
        "20251110_Rotterdam_clean_redeliver.csv",
        separator=";",
        has_header=True,
        truncate_ragged_lines=True,
    )

    skip = True
    for gmw_id in df["location_code"].to_list():
        if gmw_id == "GMW000000101148-001":
            skip = False

        if skip:
            continue

        lizard_location = (
            ls.get(f"https://rotterdam.lizard.net/api/v4/locations/?code={gmw_id}")
            .json()
            .get("results", [])[0]
        )
        gld_imbro_id = (
            lizard_location.get("extra_metadata", {}).get("bro", {}).get("gldIdImbro", None)
        )
        gld_imbroa_id = (
            lizard_location.get("extra_metadata", {}).get("bro", {}).get("gldIdImbroA", None)
        )
        gld_ids = [gld_imbro_id, gld_imbroa_id]

        for gld_id in gld_ids:
            print(f"Processing GLD ID {gld_id} for location {gmw_id}...")
            if gld_id is None:
                print(f"No GLD ID found for location {gmw_id}, skipping.")
                continue

            corrector = GLDCorrecter(gld_id)
            corrector.set_project_number("5544")
            corrector.delete_observations()


def check_bro_ids():
    import os

    from src.brostar_api_requests.brostar_api_requests import BROSTARConnection

    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    df = pl.read_csv(
        "gld_addition_bro_check2.csv", separator=";", has_header=True, truncate_ragged_lines=True
    )

    info = []
    for row in df.iter_rows(named=True):
        gmw_id = row["gmw_id"]
        tube_number = row["tube_number"]
        r = brostar.get(
            "gld/glds",
            params={"gmw_bro_id": gmw_id, "tube_number": tube_number, "quality_regime": "IMBRO/A"},
        )

        new_gld_id = r.json().get("results", [])
        if len(new_gld_id) > 0:
            new_gld_id = new_gld_id[0].get("bro_id", None)
        else:
            new_gld_id = None

        if not new_gld_id:
            try:
                logger.warning(f"No GLD found for GMW ID {gmw_id} and tube number {tube_number}.")
                info_entry = {**row, "fixed": False, "reason": "No GLD found"}
                info.append(info_entry)
            except Exception:
                continue
            continue

        upload_task = brostar.get_detail("uploadtasks", row["uuid"])
        upload_task_info = upload_task.json()
        url = upload_task.url
        metadata = upload_task_info.get("metadata", {})
        metadata["broId"] = new_gld_id
        metadata["qualityRegime"] = "IMBRO/A"
        metadata["requestReference"] = metadata["requestReference"].replace(
            row["bro_id"], new_gld_id
        )

        try:
            r = brostar.s.patch(
                url,
                json={
                    "metadata": metadata,
                    "status": "PENDING",
                },
            )
            r.raise_for_status()
            logger.info(f"Updated upload task {row['uuid']} with new GLD ID {new_gld_id}.")

            info_entry = {**row, "fixed": True, "reason": ""}
            info.append(info_entry)
        except Exception as e:
            logger.error(f"Failed to update upload task {row['uuid']}: {e}")
            info_entry = {**row, "fixed": False, "reason": str(e)}
            info.append(info_entry)

    df_info = pl.DataFrame(info)
    df_info.write_csv("gld_addition_bro_check_fixed3.csv")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    check_status_processing_upload_tasks()
