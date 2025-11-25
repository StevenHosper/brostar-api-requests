import math
import os
from pprint import pprint

import pandas as pd

from src.brostar_api_requests.connection import BROSTARConnection

request_reference = "dummy"
request_type = "registration"


def select_data_model(row):
    data_model = None

    # select data model based on gebeurtenis_type
    if row["gebeurtenis_type"] == "GMW_PositionsMeasuring":
        data_model = build_positions_measuring_data_model(row)

    elif row["gebeurtenis_type"] == "GMW_Lengthening":
        data_model = build_lengthening_data_model(row)

    elif row["gebeurtenis_type"] == "GMW_WellHeadProtector":
        data_model = build_wellhead_protector_data_model(row)

    elif row["gebeurtenis_type"] == "GMW_GroundLevelMeasuring":
        data_model = build_groundlevelmeasuring_data_model(row)

    elif row["gebeurtenis_type"] == "GMW_TubeStatus":
        data_model = build_tubestatus_data_model(row)

    elif row["gebeurtenis_type"] == "GMW_Shortening":
        data_model = build_shortening_data_model(row)

    elif row["gebeurtenis_type"] == "GMW_Positions":
        data_model = build_positions_data_model(row)

    return data_model


def turn_data_model_into_request(sourcedocument, row):
    quality_regime = None
    time = row.get("gebeurtenis_datum")
    # turn into datetime format YYYY-MM-DDTHH:MM:SSZ
    time = pd.to_datetime(time)
    imbroa_switch_moment = pd.to_datetime("2018-01-01T00:00:00Z")
    if time < imbroa_switch_moment:
        quality_regime = "IMBRO/A"
    else:
        quality_regime = "IMBRO"

    metadata = {
        "broId": row.get("gmw_id"),
        "qualityRegime": quality_regime,
        "deliveryAccountableParty": f"{int(row.get('kvk_nummer')):08d}",
        "requestReference": request_reference,
    }

    if "monitoring_tubes" in sourcedocument:
        monitoring_tubes = sourcedocument.pop("monitoring_tubes")
        camel_tubes = snake2camel(monitoring_tubes)
        sourcedocument["monitoringTubes"] = camel_tubes

    sourcedocument_data = snake2camel(sourcedocument)

    payload = {
        "bro_domain": "GMW",
        "project_number": row.get("project_number"),
        "request_type": request_type,
        "registration_type": row.get("gebeurtenis_type"),
        "sourcedocument_data": sourcedocument_data,
        "metadata": metadata,
    }
    print(payload)

    return payload


# ------------------------------------------------------------------
# below are functions to build data models for each gebeurtenis_type
# ------------------------------------------------------------------


def build_positions_measuring_data_model(row):
    """
    pydantic classes of model:
    class GMWPositionsMeasuring(GMWEvent):
        monitoring_tubes: list[MonitoringTubePositions]
        ground_level_position: float | None = None
        ground_level_positioning_method: str | None = None

    and the MonitoringTubePositions class:
    class MonitoringTubePositions(CamelModel):
        tube_number: int
        tube_top_position: float
        tube_top_positioning_method: str
    """

    sourcedocument = {
        "monitoring_tubes": [
            {
                "tube_number": int(row.get("buis_nr", 1)),
                "tube_top_position": row.get("positie_bovenkant_buis", None),
                "tube_top_positioning_method": row.get("methode_bkb", None),
            }
        ],
        "ground_level_position": row.get("positie_maaiveld", None),
        "ground_level_positioning_method": row.get("methode_maaiveld", None),
    }

    return sourcedocument


def build_lengthening_data_model(row):
    """
    class MonitoringTubeLengthening(CamelModel):
        tube_number: int
        variable_diameter: str = "ja"
        tube_top_diameter: int | None = None
        tube_top_position: float
        tube_top_positioning_method: str
        tube_material: str | None = None
        glue: str | None = None
        plain_tube_part_length: float
    """

    sourcedocument = {
        "tube_number": int(row.get("buis_nr", 1)),
        "variable_diameter": None,
        "tube_top_diameter": None,
        "tube_top_position": row.get("positie_bovenkant_buis", None),
        "tube_top_positioning_method": row.get("methode_bkb", None),
        "tube_material": None,
        "glue": None,
        "plain_tube_part_length": row.get("lengte_stijgbuisdeel", None),
    }

    return sourcedocument


def build_wellhead_protector_data_model(row):
    """
    class GMWWellHeadProtector(GMWEvent):
        well_head_protector: str
    """

    sourcedocument = {
        "well_head_protector": row.get("beschermconstructie", None),
    }

    return sourcedocument


def build_groundlevelmeasuring_data_model(row):
    """
    class GMWGroundLevelMeasuring(GMWEvent):
        ground_level_position: float
        ground_level_positioning_method: str
    """

    sourcedocument = {
        "ground_level_position": row.get("positie_maaiveld", None),
        "ground_level_positioning_method": row.get("methode_maaiveld", None),
    }

    return sourcedocument


def build_tubestatus_data_model(row):
    """
    class GMWTubeStatus(GMWEvent):
        monitoring_tubes: list[MonitoringTubeStatus]

    class MonitoringTubeStatus(CamelModel):
        tube_number: int
        tube_status: str
    """

    sourcedocument = {
        "monitoring_tubes": [
            {"tube_number": int(row.get("buis_nr", 1)), "tube_status": row.get("tube_status", None)}
        ],
    }

    return sourcedocument


def build_shortening_data_model(row):
    """
    class MonitoringTubeShortening(CamelModel):
        tube_number: int
        tube_top_position: float
        tube_top_positioning_method: str
        plain_tube_part_length: float
    """

    sourcedocument = {
        "monitoring_tubes": [
            {
                "tube_number": int(row.get("buis_nr", 1)),
                "tube_top_position": row.get("positie_bovenkant_buis", None),
                "tube_top_positioning_method": row.get("methode_bkb", None),
                "plain_tube_part_length": row.get("lengte_stijgbuisdeel", None),
            }
        ]
    }

    return sourcedocument


def build_positions_data_model(row):
    """
    class GMWPositions(GMWEvent):
        well_stability: str = "nee"
        ground_level_stable: str = "instabiel"
        ground_level_position: float
        ground_level_positioning_method: str
        monitoring_tubes: list[MonitoringTubePositions]


    class MonitoringTubePositions(CamelModel):
        tube_number: int
        tube_top_position: float
        tube_top_positioning_method: str
    """

    sourcedocument = {
        "well_stability": row.get("put_stabiliteit", "nee"),
        "ground_level_stable": row.get("maaiveld_stabiel", "instabiel"),
        "ground_level_position": row.get("positie_maaiveld", None),
        "ground_level_positioning_method": row.get("methode_maaiveld", None),
        "monitoring_tubes": [
            {
                "tube_number": int(row.get("buis_nr", 1)),
                "tube_top_position": row.get("positie_bovenkant_buis", None),
                "tube_top_positioning_method": row.get("methode_bkb", None),
            }
        ],
    }

    return sourcedocument


def clean_nans(obj):
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    else:
        return obj


def snake2camel(obj):
    """Recursively convert snake_case keys and strings to camelCase."""

    def to_camel(s):
        parts = s.split("_")
        return parts[0] + "".join(word.capitalize() for word in parts[1:])

    if isinstance(obj, dict):
        return {to_camel(k): v for k, v in obj.items()}
    elif isinstance(obj, list):
        return [snake2camel(v) for v in obj]
    elif isinstance(obj, str):
        return to_camel(obj)
    else:
        return obj


# %%
# load csv
def load_and_format_payloads():
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)  # BROSTAR API Key
    brostar.set_website(production=True)

    df = pd.read_csv(
        r"C:\Users\steven.hosper\Downloads\20251106_AvecoDeBondt_VeluweOverzicht.csv", sep=","
    )
    df_kvk = pd.read_excel(r"C:\Users\steven.hosper\Downloads\overview_gmn_vitens_v2.xlsx")[
        ["meetnet", "deliveryAccountableParty", "projectNumber"]
    ]

    # rename 'meetnet' to 'organisation'
    # rename 'deliveryAccountableParty' to 'kvk_nummer'
    df_kvk = df_kvk.rename(
        columns={
            "meetnet": "organisation",
            "deliveryAccountableParty": "kvk_nummer",
            "projectNumber": "project_number",
        }
    )

    print(df)
    print(df_kvk)
    # merge df with df_kvk on 'organisation'
    df = pd.merge(df, df_kvk, on="organisation", how="left").drop_duplicates()
    payloads = []
    print(len(df))
    for i, row in df.iterrows():
        if row["gebeurtenis_type"] == "GMW_Positions":
            row["gebeurtenis_type"] = "GMW_PositionsMeasuring"

        if i < 13529:
            continue
        print(row)
        print(f"Processing row {i + 1} of {len(df)}")
        sourcedocument = select_data_model(row)
        sourcedocument = clean_nans(sourcedocument)
        sourcedocument["event_date"] = row.get("gebeurtenis_datum").split("T")[0]
        payload = turn_data_model_into_request(sourcedocument, row)
        r = brostar.post_upload(payload=payload, is_json=True)
        print(f"Response status code: {r.status_code}")
        print(f"Response content: {r.content}")

        payloads.append(payload)
        pprint(payload)
        # break

    print(f"Total payloads created: {len(payloads)}")


# %%
