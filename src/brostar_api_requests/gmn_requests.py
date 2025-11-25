import os

import polars as pl

from .connection import BROSTARConnection
from .upload_models import (
    GMNMeasuringPoint,
    GMNStartregistration,
    MeasuringPoint,
    UploadTask,
    UploadTaskMetadata,
)


def deliver_gmn_start_registration(file_location: str, measuring_point_location: str):
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    df = pl.read_csv(
        rf"{file_location}", separator=",", has_header=True, truncate_ragged_lines=True
    )
    df_meetpunten = pl.read_csv(
        rf"{measuring_point_location}", separator=",", has_header=True, truncate_ragged_lines=True
    )
    print(df)

    for row in df.iter_rows(named=True):
        if row["name"] == "Prov GE - GLD - Waterwetvergunning Aalten":
            print("skip aalten")
            continue

        meetpunten_filtered = df_meetpunten.filter(
            pl.lit(row["name"]).str.contains(pl.col("meetnet")), pl.col("broId").is_not_null()
        )

        print(meetpunten_filtered.head())

        if meetpunten_filtered.height == 0:
            print(f"No measuring points found for {row['name']}, skipping...")
            with open("gmn_startregistrations_log.txt", "a") as log_file:
                log_file.write(f"{row['name']},Geen meetpunten\n")
            continue

        meetpunten_filtered = meetpunten_filtered.with_columns(
            pl.col("constructionDate")
            .str.strptime(pl.Date, format="%Y-%m-%d")
            .alias("constructionDate"),
        ).sort("constructionDate", descending=False)
        first_meetpunt = meetpunten_filtered.row(0, named=True)

        print(first_meetpunt)

        first_meetpunten = meetpunten_filtered.filter(
            pl.col("constructionDate") == first_meetpunt["constructionDate"]
        )
        first_meetpunten = first_meetpunten.unique(
            subset=["measuringPointCode", "broId", "tubeNumber"]
        )

        metadata = UploadTaskMetadata(
            request_reference=f"{row['name']}",
            delivery_accountable_party="05069581",
            quality_regime="IMBRO/A",
        )

        sourcedocument_data = GMNStartregistration(
            object_id_accountable_party=row["name"],
            name=row["name"],
            delivery_context=row["deliveryContext"],
            monitoring_purpose=row["monitoringPurpose"],
            groundwater_aspect=row["groundwaterAspect"],
            start_date_monitoring=first_meetpunt["constructionDate"].strftime("%Y-%m-%d"),
            measuring_points=[
                MeasuringPoint(
                    measuring_point_code=meetpunten_filtered["measuringPointCode"],
                    bro_id=meetpunten_filtered["broId"],
                    tube_number=meetpunten_filtered["tubeNumber"],
                )
                for meetpunten_filtered in first_meetpunten.iter_rows(named=True)
            ],
        )

        upload_task = UploadTask(
            bro_domain="GMN",
            project_number="6708",
            registration_type="GMN_StartRegistration",
            request_type="insert",
            metadata=metadata,
            sourcedocument_data=sourcedocument_data,
        )

        r = brostar.post_upload(
            payload=upload_task.model_dump(mode="json", by_alias=True), is_json=True
        )
        if r.status_code < 300:
            bro_id = brostar.await_bro_id(r.json()["uuid"])
            print(f"For gmn: {row['name']}")
            print(f"Successfully uploaded GMN start registration. Bro ID: {bro_id}")
        else:
            print(r.content)
            r.raise_for_status()

        with open("gmn_startregistrations_log_hist.txt", "a") as log_file:
            log_file.write(f"{row['name']},{bro_id}\n")


def deliver_gmn_measuring_points(file_location: str):
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    df = pl.read_csv(
        rf"{file_location}", separator=",", has_header=True, truncate_ragged_lines=True
    )
    df = df.filter((pl.col("broId").is_not_null()) & (pl.col("broId") != ""))
    df = df.with_columns(
        pl.col("startDateMonitoring")
        .str.strptime(pl.Date, format="%Y-%m-%d")
        .alias("startDateMonitoringDt"),
    ).sort("startDateMonitoringDt", descending=False)
    print(df.head())

    last_bro_name = ""
    skip = True
    for row in df.iter_rows(named=True):
        bro_name = f"Prov GE - GLD - {row['meetnet']}"
        print(bro_name)
        if row["measuringPointCode"] == "GMW46B000029002":
            skip = False

        # Historisch skip GMW27D000001001
        if skip:
            continue
        # Commercieel skip B40F0488001

        print(bro_name)
        if bro_name != last_bro_name:
            last_bro_name = bro_name.strip()
            r = brostar.get("gmn/gmns", params={"internal_id": last_bro_name})
            r.raise_for_status()
            gmn_id = r.json()["results"][0]["bro_id"]

            if r.json()["count"] == 0:
                print(f"No GMN found for {bro_name}, skipping...")
                continue

        metadata = UploadTaskMetadata(
            bro_id=gmn_id,
            request_reference=f"{row['measuringPointCode']}",
            delivery_accountable_party="05069581",
            quality_regime="IMBRO/A",
            correction_reason="eigenCorrectie",
        )

        sourcedocument_data = GMNMeasuringPoint(
            bro_id=row["broId"],
            measuring_point_code=row["measuringPointCode"]
            if not row["measuringPointCode"].startswith("GMW")
            else row["measuringPointCode"][3:],
            tube_number=row["tubeNumber"],
            event_date=row["startDateMonitoring"],
        )

        upload_task = UploadTask(
            bro_domain="GMN",
            project_number="6708",
            registration_type="GMN_MeasuringPoint",
            request_type="registration",
            metadata=metadata,
            sourcedocument_data=sourcedocument_data,
        )

        print(upload_task.model_dump(mode="json", by_alias=True))
        r = brostar.post_upload(
            payload=upload_task.model_dump(mode="json", by_alias=True), is_json=True
        )
        if r.status_code < 300:
            bro_id = brostar.await_bro_id(r.json()["uuid"])
            print(f"Successfully uploaded GMN start registration. Bro ID: {bro_id}")
        else:
            print(r.content)
            r.raise_for_status()


def deliver_gelderland_gmn():
    # deliver_gmn_start_registration(file_location="meetnetten_payloads.csv", measuring_point_location="measuring_points_payloads.csv")
    deliver_gmn_measuring_points(
        file_location=r"C:\Users\steven.hosper\Downloads\measuring_points_payloads.csv"
    )  # C:\Users\steven.hosper\Downloads\
