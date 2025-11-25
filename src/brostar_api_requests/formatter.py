from .connection import BROSTARConnection
from .upload_models import (
    Electrode,
    GeoOhmCable,
    GMWConstruction,
    MonitoringTube,
)


def format_electrodes(electrodes_data: list[dict[str, str]]) -> list[Electrode]:
    return [Electrode(**e) for e in electrodes_data]


def format_geo_ohm_cables(cables_data: list[dict[str, str]]) -> list[GeoOhmCable] | None:
    geo_ohm_cables = []
    for cable in cables_data:
        electrodes_list = cable.pop("electrodes", [])
        geo_ohm_cables.append(
            GeoOhmCable(
                **cable,
                electrodes=format_electrodes(electrodes_list),
            )
        )
    return geo_ohm_cables if geo_ohm_cables else None


def format_monitoring_tubes(tubes_data: list[dict[str, str]]) -> list[MonitoringTube]:
    monitoring_tubes = []
    for tube in tubes_data:
        geo_ohm_cable_list = tube.pop("geo_ohm_cables", [])
        monitoring_tubes.append(
            MonitoringTube(
                **tube,
                geo_ohm_cables=format_geo_ohm_cables(geo_ohm_cable_list),
            )
        )
    return monitoring_tubes


def build_gmw_construction(
    gmw_data: dict[str, str], monitoring_tubes_data: dict[str, str]
) -> GMWConstruction:
    return GMWConstruction(
        **gmw_data,
        monitoringTubes=format_monitoring_tubes(monitoring_tubes_data),
    )


class PayloadFormatter:
    def __init__(self, brostar: BROSTARConnection) -> None:
        self.brostar = brostar

    def format_gmw_construction(self, gmw_bro_id: str) -> GMWConstruction:
        """Based on a BRO-ID retrieve all information for a construction"""
        # Get the main GMW data
        r = self.brostar.get("gmw/gmws", params={"bro_id": gmw_bro_id})
        gmw_results = r.json()["results"]

        # Check if we found any results
        if not gmw_results or len(gmw_results) == 0:
            raise ValueError(f"No GMW found with BRO-ID: {gmw_bro_id}")

        # Get the first (and should be only) result
        gmw_data = gmw_results[0]

        # Get all monitoring tubes for this GMW
        r = self.brostar.get("gmw/monitoringtubes", params={"gmw_bro_id": gmw_bro_id})
        monitoring_tubes_data = r.json()["results"]

        gmw_construction = build_gmw_construction(
            gmw_data=gmw_data, monitoring_tubes_data=monitoring_tubes_data
        )

        return gmw_construction
