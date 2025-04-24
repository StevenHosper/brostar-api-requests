from typing import Literal

from .connection import BROSTARConnection
from .upload_models import (
    Electrode,
    GeoOhmCable,
    GMWConstruction,
    MonitoringTube,
    UploadTaskMetadata,
)

RequestTypeOptions = Literal["registration", "replace", "insert", "move", "delete"]
RegistrationTypeOptions = Literal["GMW_Construction"]


class PayloadFormatter:
    def __init__(self, brostar: BROSTARConnection) -> None:
        self.brostar = brostar

    def format_metadata(
        self,
        request_reference: str,
        delivery_accountable_party: str,
        bro_id: str,
        quality_regime: Literal["IMBRO", "IMBRO/A"],
    ) -> UploadTaskMetadata:
        return UploadTaskMetadata(
            requestReference=request_reference,
            deliveryAccountableParty=delivery_accountable_party,
            qualityRegime=quality_regime,
            broId=bro_id,
            correctionReason="eigenCorrectie",
        )

    def format_gmw_construction(
        self, gmw_bro_id: str
    ) -> tuple[GMWConstruction, UploadTaskMetadata]:
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

        # Format monitoring tubes
        monitoring_tubes = []
        for tube_data in monitoring_tubes_data:
            # Format GeoOhmCables for this tube if they exist
            geo_ohm_cables = []
            if tube_data.get("geo_ohm_cables"):
                for cable_data in tube_data["geo_ohm_cables"]:
                    # Format electrodes for this cable
                    electrodes = []
                    for electrode_data in cable_data.get("electrodes", []):
                        electrode = Electrode(
                            electrodeNumber=electrode_data["electrode_number"],
                            electrodePackingMaterial=electrode_data["electrode_packing_material"],
                            electrodeStatus=electrode_data["electrode_status"],
                            electrodePosition=electrode_data["electrode_position"],
                        )
                        electrodes.append(electrode)

                    geo_ohm_cable = GeoOhmCable(
                        cableNumber=cable_data["cable_number"], electrodes=electrodes
                    )
                    geo_ohm_cables.append(geo_ohm_cable)

            # Create the monitoring tube object
            monitoring_tube = MonitoringTube(
                tubeNumber=tube_data["tube_number"],
                tubeType=tube_data["tube_type"],
                artesianWellCapPresent=tube_data["artesian_well_cap_present"],
                sedimentSumpPresent=tube_data["sediment_sump_present"],
                numberOfGeoOhmCables=tube_data["number_of_geo_ohm_cables"],
                tubeTopDiameter=tube_data.get("tube_top_diameter"),
                variableDiameter=tube_data["variable_diameter"],
                tubeStatus=tube_data["tube_status"],
                tubeTopPosition=tube_data["tube_top_position"],
                tubeTopPositioningMethod=tube_data["tube_top_positioning_method"],
                tubePackingMaterial=tube_data["tube_packing_material"],
                tubeMaterial=tube_data["tube_material"],
                glue=tube_data["glue"],
                screenLength=tube_data["screen_length"],
                screenProtection=tube_data.get("screen_protection"),
                sockMaterial=tube_data["sock_material"],
                plainTubePartLength=tube_data["plain_tube_part_length"],
                sedimentSumpLength=tube_data.get("sediment_sump_length"),
                geoOhmCables=geo_ohm_cables if geo_ohm_cables else None,
            )
            monitoring_tubes.append(monitoring_tube)

        # Create the final GMWConstruction object
        gmw_construction = GMWConstruction(
            objectIdAccountableParty=gmw_data["delivery_accountable_party"],
            deliveryContext=gmw_data["delivery_context"],
            constructionStandard=gmw_data["construction_standard"],
            initialFunction=gmw_data["initial_function"],
            numberOfMonitoringTubes=gmw_data["nr_of_monitoring_tubes"],
            groundLevelStable=gmw_data["ground_level_stable"],
            wellStability=gmw_data.get("well_stability"),
            owner=gmw_data.get("owner"),
            maintenanceResponsibleParty=None,  # This field doesn't appear in the API response
            wellHeadProtector=gmw_data["well_head_protector"],
            wellConstructionDate=gmw_data["well_construction_date"],
            deliveredLocation=gmw_data["delivered_location"],
            horizontalPositioningMethod=gmw_data["horizontal_positioning_method"],
            localVerticalReferencePoint=gmw_data["local_vertical_reference_point"],
            offset=gmw_data["offset"],
            verticalDatum=gmw_data["vertical_datum"],
            groundLevelPosition=gmw_data.get("ground_level_position"),
            groundLevelPositioningMethod=gmw_data["ground_level_positioning_method"],
            monitoringTubes=monitoring_tubes,
        )

        metadata = self.format_metadata(
            bro_id=gmw_bro_id,
            quality_regime=gmw_data["quality_regime"],
            request_reference=gmw_data.get("intern_id", f"{gmw_bro_id}"),
            delivery_accountable_party=gmw_data["delivery_accountable_party"],
        )

        return gmw_construction, metadata
