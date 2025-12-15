import datetime
import logging
import os

import polars as pl
import requests

from brostar_api_requests.brostar_api_requests import (
    deliver_frd_start_registration,
    setup_lizard_session,
)
from brostar_api_requests.connection import BROSTARConnection

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


def main():
    # business_main()
    from src.brostar_api_requests.connection import BROSTARConnection

    brostar = BROSTARConnection()
    brostar.set_website(production=False)
    brostar.s.headers.update(
        {
            "Authorization": "Bearer eyJraWQiOiJPN2xjQVFNM2JBT0R0RDRLVXIrMER0QjZCaFlyT0ViVG4zK1lWa1wvNzdURT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI2bGZuaGttOXQwN3FsMHNuaGxxMzJlYmRucSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoic3RhZ2luZy5icm9zdGFyLm5sXC8qOnJlYWR3cml0ZSIsImF1dGhfdGltZSI6MTc2NTQ2MTc4NiwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LXdlc3QtMS5hbWF6b25hd3MuY29tXC9ldS13ZXN0LTFfdlB3WE9uTmJpIiwiZXhwIjoxNzY1NDY1Mzg2LCJpYXQiOjE3NjU0NjE3ODYsInZlcnNpb24iOjIsImp0aSI6ImZkYmQwZGMxLWRmMTAtNDYxYS1iYzg2LTJhNzVhZTcwNjM5OSIsImNsaWVudF9pZCI6IjZsZm5oa205dDA3cWwwc25obHEzMmViZG5xIn0.LclwZOtOTHXhX5vVXeFv7yBF_NsFQdRNcNo_5Uckx1ehadBOkqIryKRU4hQdZ80wL_7ye5_wXATyq7yLag1ML4xbqf9mALQxakEeDe8ZPCfB7eAE5ZpfHzpjhf-FOHHdeXlDAFMVNBIcNZ0bv3nKIXOOO8dbSB6o2Ta56VQZmT3PQlqMpxONtqkqBUmfc4TkOH_s3WcJQ5myRl6bPPZNU-wYw6lieqgGJVzheQ_NhDQiTtzzbZJN3Eg1ruMCtLpsihbyQ4ys3x-3PbAQuIN9T-HYnFt2u3S7zgofZaWwi2ynWyroonNxlp9lVc1i6zUt9gTNGB8yDC3JbM3hrKyvDw"
        }
    )
    # brostar.refresh_access_token("6lfnhkm9t07ql0snhlq32ebdnq", "1s1e10bm1opaknceq6u90dkusdpgls8jogb8s81kilu0pc7p119a")
    logger.info(brostar.s.headers)
    logger.info(brostar.check_token_validity())

    r = brostar.s.get("https://staging.brostar.nl/api/uploadtasks/")
    logger.info(r.json())


def ingest_dino_csv(filepath: str):
    ls = setup_lizard_session()

    df = pl.read_csv(filepath, separator=";", has_header=True, truncate_ragged_lines=True)
    print(df.columns)
    gws_name = df.item(0, 0)
    filternummer = df.item(0, 1)

    df = df.select(
        ["Peildatum", "Peiltijdstip", "Stand tov NAP", "Onbetrouwbaar", "Meetinstrument"]
    )

    valid_values = {"Betrouwbaar", "Onbetrouwbaar"}
    unique_values = set(df.select("Onbetrouwbaar").to_series().unique().to_list())

    # Check if all values are valid
    if unique_values.issubset(valid_values):
        df = df.with_columns(
            pl.when(pl.col("Onbetrouwbaar").eq("Betrouwbaar")).then(0).otherwise(7).alias("flag"),
        )
    else:
        raise ValueError(
            f"Invalid values found in 'Onbetrouwbaar' column. Expected 'Betrouwbaar' or 'Onbetrouwbaar'. Found: {df.select('Onbetrouwbaar').to_series().unique().to_list()}"
        )

    # For items that do not have time, set time to 12:00:00
    # Then create a datetime column
    df = df.with_columns(
        pl.when(pl.col("Peiltijdstip").is_null())
        .then(pl.lit("12:00:00"))
        .otherwise(pl.col("Peiltijdstip"))
        .alias("Peiltijdstip_filled"),
    )
    df = df.with_columns(
        (pl.col("Peildatum") + "T" + pl.col("Peiltijdstip_filled") + "Z").alias("time"),
        (pl.col("Stand tov NAP") / 100).alias("value"),
    )
    df = df.with_columns(
        (pl.col("time").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"))
        .dt.replace_time_zone("UTC", ambiguous="earliest")
        .alias("datetime"),
    )
    print(df.head())

    r = ls.get(f"https://vitens.lizard.net/api/v4/groundwaterstations/?name={gws_name}")
    gws = r.json().get("results", [])[0]
    for filter in gws.get("filters", []):
        if filter["code"].endswith(str(filternummer)):
            for timeserie_url in filter.get("timeseries", []):
                r = ls.get(timeserie_url)
                r.raise_for_status()

                timeserie = r.json()
                start = datetime.datetime.strptime(
                    timeserie["start"]
                    if timeserie["start"] is not None
                    else "2025-01-01T00:00:00Z",
                    "%Y-%m-%dT%H:%M:%SZ",
                ).replace(tzinfo=datetime.UTC)
                if timeserie["observation_type"]["code"] == "WNS9040":
                    df_logger = df.filter(pl.col("datetime") <= start).filter(
                        pl.col("Meetinstrument") == "Diver"
                    )
                    df_logger = df_logger.select(["time", "value", "flag"])
                    print(
                        f"Groundwater station: {gws_name}, filter number: {filternummer} (logger)"
                    )
                    print(df_logger.head(5))
                    ls.post(timeserie["url"] + "events/", json=df_logger.to_dicts())

                elif timeserie["observation_type"]["code"] == "WNS9040.hand":
                    df_logger = df.filter(pl.col("datetime") <= start).filter(
                        pl.col("Meetinstrument").is_null()
                    )
                    df_logger = df_logger.select(["time", "value", "flag"])
                    print(
                        f"Groundwater station: {gws_name}, filter number: {filternummer} (manual)"
                    )
                    print(df_logger.head(5))
                    ls.post(timeserie["url"] + "events/", json=df_logger.to_dicts())


def get_gmw_id_tube_nr(gld_id: str, brostar: BROSTARConnection):
    r = brostar.get("gld/glds", params={"bro_id": gld_id})
    r.raise_for_status()
    results = r.json().get("results", [])
    if len(results) == 0:
        logger.warning(f"No GLD found for GLD ID {gld_id}.")
        return None, None

    gmw_bro_id = results[0].get("gmw_bro_id", None)
    tube_number = results[0].get("tube_number", None)
    return gmw_bro_id, tube_number


def add_gmw_check_procedures():
    df = pl.read_csv("20251215_Rotterdam.csv")

    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    ls = setup_lizard_session()
    info = []
    for row in df.iter_rows(named=True):
        gmw_id, tube_number = get_gmw_id_tube_nr(row["broId"], brostar)

        # get procedures from timeserie
        r = ls.get(
            f"https://rotterdam.lizard.net/api/v4/timeseries/?location__code={gmw_id}-{str(tube_number).zfill(3)}&observation_type__code=WNS9040.hand"
        )
        r.raise_for_status()
        logger.info(r.url)
        logger.info(r.json())
        timeseries = r.json().get("results", [])
        if len(timeseries) == 0:
            logger.warning(f"No timeserie found for GMW ID {gmw_id} and tube number {tube_number}.")
            continue

        metadata = timeseries[0].get("extra_metadata", {}).get("bro", {})
        # bool check if a controle measurement is present in
        controle = any([proc.get("observationtype", "") == "controlemeting" for proc in metadata])
        logger.info(
            f"Procedures for {gmw_id}-{tube_number}: {metadata}, controlemeting present: {controle}"
        )
        info.append(
            {
                "broId": row["broId"],
                "gmw_bro_id": gmw_id,
                "tube_number": tube_number,
                "observationId": row["observationId"],
                "measurements_count": row["tvpCount"],
                "procedures": str(metadata),
                "controlemeting_present": controle,
            }
        )

    df_info = pl.DataFrame(info)
    df_info.write_csv("20251215_Rotterdam_procedures_check.csv")


def delete_and_adjust_rotterdam():
    df = pl.read_csv("20251215_Rotterdam_procedures_check.csv", separator=";")

    from src.brostar_api_requests.brostar_api_requests import GLDCorrecter

    correct = GLDCorrecter()

    for row in df.iter_rows(named=True):
        correct.set_bro_id(row["broId"])
        correct.set_project_number("5544")
        correct.delete_observation(row["observationId"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    delete_and_adjust_rotterdam()
    # from src.brostar_api_requests.brostar_api_requests import bulk_gmw_construction_request
    # bulk_gmw_construction_request(excel_file=r"C:\Users\steven.hosper\Desktop\PythonPackages\BrostarAPI\input\20251212_hendrik_test_out.xlsx", kvk="51640813")

    # For every file in the input folder (/input), with extension .csv, ingest the file
    # import os
    # input_folder = "input"
    # for filename in os.listdir(input_folder):
    #     if filename.endswith(".csv"):
    #         filepath = os.path.join(input_folder, filename)
    #         print(f"Ingesting file: {filepath}")
    #         ingest_dino_csv(filepath)

    # main()
    # check_status_processing_upload_tasks()

    # from src.brostar_api_requests.brostar_api_requests import ingest_gld_ids_into_lizard
    # ingest_gld_ids_into_lizard()

    # from src.brostar_api_requests.data_retriever.bro_xml_reader import GARXML
    # from src.brostar_api_requests.gar_requests import correct_gar_tube

    # gars_to_correct1 = ["GAR000000041890", "GAR000000041948", "GAR000000041990", "GAR000000042046", "GAR000000042060", "GAR000000042100", "GAR000000042101"]
    # gars_to_correct2 = ["GAR000000041983"]
    # for gar in gars_to_correct1:
    #     correct_gar_tube(bro_id=gar, gmw_id="GMW000000079662", tube_number=1, correctie_reden="eigenCorrectie", projectnummer="5459")

    # for gar in gars_to_correct2:
    #     correct_gar_tube(bro_id=gar, gmw_id="GMW000000079662", tube_number=2, correctie_reden="eigenCorrectie", projectnummer="5459")
