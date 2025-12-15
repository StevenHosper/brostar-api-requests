import time
import xml.etree.ElementTree as ET

import requests

from ..upload_models import Analysis, AnalysisProcess, LaboratoryAnalysis
from .namespaces import (
    ns_reg_gar,
    ns_reg_gld_tot,
    ns_reg_gmn_tot,
    ns_reg_gmw,
)


def _request_bro_xml(bro_id: str, query_params: str, type: str, bro_url: str) -> ET.Element | None:
    options = ["gmw", "frd", "gar", "gmn", "gld"]
    if type.lower() not in options:
        raise Exception(f"Unknown type: {type}. Use a correct option: {options}.")

    retry = 0
    while retry < 3:
        # Try to get a response with statuscode 200 (deal with temporary time-out of servicedesk)
        res = requests.get(f"{bro_url}gm/{type}/v1/objects/{bro_id}?{query_params}")

        if res.status_code < 300:
            return ET.fromstring(res.content)

        retry += 1
        time.sleep(10)
    res.raise_for_status()
    return


class GMWXML:
    def __init__(
        self,
        bro_id: str,
        bro_url: str,
        full_history: bool = True,
    ) -> None:
        fh = "ja" if full_history else "nee"
        if not isinstance(bro_id, str):
            raise TypeError(f"Incorrect type: {type(bro_id)}.")
        elif bro_id.startswith("GMW") and bro_id.split("GMW")[-1].isdigit() and len(bro_id) == 15:
            self.xml_etree = _request_bro_xml(bro_id, f"fullHistory={fh}", "gmw", bro_url)
        else:
            raise ValueError(f"Incorrect GMW-ID: {bro_id}")

    @property
    def bro_id(self) -> str | None:
        return self.xml_etree.find(".//brocom:broId", ns_reg_gmw).text

    @property
    def delivery_accountable_party(self) -> str | None:
        return self.xml_etree.find(".//brocom:deliveryAccountableParty", ns_reg_gmw).text

    @property
    def quality_regime(self) -> str | None:
        return self.xml_etree.find(".//brocom:qualityRegime", ns_reg_gmw).text

    @property
    def delivery_context(self) -> str | None:
        return self.xml_etree.find(".//deliveryContext", ns_reg_gmw).text

    @property
    def construction_standard(self) -> str | None:
        return self.xml_etree.find(".//constructionStandard", ns_reg_gmw).text

    @property
    def initial_function(self) -> str | None:
        return self.xml_etree.find(".//initialFunction", ns_reg_gmw).text

    @property
    def maintenance_responsible_party(self) -> str | None:
        try:
            return self.xml_etree.find(".//maintenanceResponsibleParty", ns_reg_gmw).text
        except AttributeError:
            return None

    @property
    def ground_level_stable(self) -> str | None:
        return self.xml_etree.find(".//groundLevelStable", ns_reg_gmw).text

    @property
    def well_stability(self) -> str | None:
        try:
            return self.xml_etree.find(".//wellStability", ns_reg_gmw).text
        except AttributeError:
            return None

    @property
    def nitg_code(self) -> str | None:
        try:
            return self.xml_etree.find(".//nitgCode", ns_reg_gmw).text
        except AttributeError:
            return None

    @property
    def well_code(self) -> str | None:
        return self.xml_etree.find(".//wellCode", ns_reg_gmw).text

    @property
    def owner(self) -> str | None:
        return self.xml_etree.find(".//owner", ns_reg_gmw).text

    @property
    def removed(self) -> str | None:
        return self.xml_etree.find(".//removed", ns_reg_gmw).text

    @property
    def well_head_protector(self) -> str | None:
        return self.xml_etree.find(".//wellHeadProtector", ns_reg_gmw).text

    def _location_srs(self, element: ET.Element) -> str | None:
        # Find the location element within the specified namespace
        location_elem = element.find("gmwcom:location", ns_reg_gmw)
        if location_elem is None:
            location_elem = element.find("brocom:location", ns_reg_gmw)

        if location_elem is not None:
            # Extract the srsName attribute value
            srs_name = location_elem.attrib.get("srsName")

            if srs_name:
                # Split the srsName string to extract the EPSG code
                epsg_code = srs_name.split("::")[-1]
                return f"EPSG:{epsg_code}"

        return None

    def _location_pos(self, element: ET.Element) -> str | None:
        return element.find(".//gml:pos", ns_reg_gmw).text

    def _location_horizontal_positioning_method(self, element: ET.Element) -> str | None:
        return element.find(".//gmwcom:horizontalPositioningMethod", ns_reg_gmw).text

    @property
    def delivered_location(self) -> dict:
        """
        Dictionary with CRS EPSG (28992), X, Y.
        """
        delivered_location = self.xml_etree.find(".//deliveredLocation", ns_reg_gmw)
        pos = self._location_pos(delivered_location).split(" ")
        return {
            "CRS": self._location_srs(delivered_location),
            "X": pos[0],
            "Y": pos[1],
            "horizontalPositioningMethod": self._location_horizontal_positioning_method(
                delivered_location
            ),
        }

    def _coordinate_transformation(self, element: ET.Element) -> str | None:
        return element.find(".//brocom:coordinateTransformation", ns_reg_gmw).text

    @property
    def standardized_location(self) -> dict:
        """
        Dictionary with the converted EPSG:4258, Lat, Lon, Transformation.
        """
        standardized_location = self.xml_etree.find(".//standardizedLocation", ns_reg_gmw)
        srs = self._location_srs(standardized_location)
        pos = self._location_pos(standardized_location).split(" ")
        transform = self._coordinate_transformation(standardized_location)
        return {
            "CRS": srs,
            "Lat": pos[0],
            "Lon": pos[1],
            "Transformation": transform,
        }

    def _delivered_vertical_position(self) -> ET.Element | None:
        return self.xml_etree.find(".//deliveredVerticalPosition", ns_reg_gmw)

    @property
    def vertical_ref_point(self) -> str | None:
        element = self._delivered_vertical_position()
        if element is not None:
            return element.find(".//gmwcom:localVerticalReferencePoint", ns_reg_gmw).text

    @property
    def offset(self) -> str | None:
        element = self._delivered_vertical_position()
        if element is not None:
            return element.find(".//gmwcom:offset", ns_reg_gmw).text

    @property
    def vertical_datum(self) -> str | None:
        element = self._delivered_vertical_position()
        if element is not None:
            return element.find(".//gmwcom:verticalDatum", ns_reg_gmw).text

    @property
    def ground_level_pos(self) -> str | None:
        element = self._delivered_vertical_position()
        if element is not None:
            return element.find(".//gmwcom:groundLevelPosition", ns_reg_gmw).text

    @property
    def ground_level_pos_method(self) -> str | None:
        element = self._delivered_vertical_position()
        if element is not None:
            return element.find(".//gmwcom:groundLevelPositioningMethod", ns_reg_gmw).text

    def _tube_number(self, element: ET.Element) -> str | None:
        return element.find(".//tubeNumber", ns_reg_gmw).text

    def _tube_type(self, element: ET.Element) -> str | None:
        return element.find(".//tubeType", ns_reg_gmw).text

    def _tube_status(self, element: ET.Element) -> str | None:
        return element.find(".//tubeStatus", ns_reg_gmw).text

    def _tube_top_position(self, element: ET.Element) -> str | None:
        return element.find(".//tubeTopPosition", ns_reg_gmw).text

    def _screen_info(self, element: ET.Element) -> dict:
        screen = element.find(".//screen", ns_reg_gmw)
        return {
            "screenLength": screen.find(".//screenLength", ns_reg_gmw).text,
            "sockMaterial": screen.find(".//sockMaterial", ns_reg_gmw).text,
            "screenTopPosition": screen.find(".//screenTopPosition", ns_reg_gmw).text,
            "screenBottomPosition": screen.find(".//screenBottomPosition", ns_reg_gmw).text,
        }

    def _plain_tube_part_length(self, element: ET.Element) -> str | None:
        return element.find(".//plainTubePart/gmwcom:plainTubePartLength", ns_reg_gmw).text

    def _material_used(self, element: ET.Element) -> dict:
        material = element.find(".//materialUsed", ns_reg_gmw)
        return {
            "tubePackingMaterial": material.find(".//gmwcom:tubePackingMaterial", ns_reg_gmw).text,
            "tubeMaterial": material.find(".//gmwcom:tubeMaterial", ns_reg_gmw).text,
            "glue": material.find(".//gmwcom:glue", ns_reg_gmw).text,
        }

    def _electrode_info(self, element: ET.Element) -> dict:
        electrode_data = {
            "electrodeNumber": element.find(".//gmwcom:electrodeNumber", ns_reg_gmw).text,
            "electrodePackagingMaterial": element.find(
                ".//gmwcom:electrodePackingMaterial", ns_reg_gmw
            ).text,
            "electrodeStatus": element.find(".//gmwcom:electrodeStatus", ns_reg_gmw).text,
            "electrodePosition": element.find(".//gmwcom:electrodePosition", ns_reg_gmw).text,
        }
        return electrode_data

    def _geo_ohm_cable_info(self, element: ET.Element) -> dict:
        geo_ohm_cable_data = {
            "cableNumber": element.find(".//cableNumber", ns_reg_gmw).text,
            "cableInUse": element.find(".//cableInUse", ns_reg_gmw).text,
            "electrodes": [],
        }

        electrodes = element.findall(".//electrode", ns_reg_gmw)
        for electrode in electrodes:
            geo_ohm_cable_data["electrodes"].append(self._electrode_info(electrode))

        geo_ohm_cable_data["numberOfElectrodes"] = len(electrodes)

        return geo_ohm_cable_data

    @property
    def number_of_monitoring_tubes(self) -> int | None:
        return int(self.xml_etree.find(".//numberOfMonitoringTubes", ns_reg_gmw).text)

    def _monitoring_tube_info(self, element: ET.Element) -> dict:
        tube_info = {
            "tubeNumber": self._tube_number(element),
            "tubeType": self._tube_type(element),
            "tubeStatus": self._tube_status(element),
            "tubeTopDiameter": element.find(".//tubeTopDiameter", ns_reg_gmw).text,
            "tubeTopPosition": self._tube_top_position(element),
            "tubeTopPositioningMethod": element.find(
                ".//tubeTopPositioningMethod", ns_reg_gmw
            ).text,
            "plainTubePartLength": self._plain_tube_part_length(element),
            "artesianWellCapPresent": element.find(".//artesianWellCapPresent", ns_reg_gmw).text,
            "sedimentSumpPresent": element.find(".//sedimentSumpPresent", ns_reg_gmw).text,
            "numberOfGeoOhmCables": int(element.find(".//numberOfGeoOhmCables", ns_reg_gmw).text),
            "variableDiameter": element.find(".//variableDiameter", ns_reg_gmw).text,
            "tubePartInserted": element.find(".//tubePartInserted", ns_reg_gmw).text,
            "tubeInUse": element.find(".//tubeInUse", ns_reg_gmw).text,
        }

        tube_info.update(self._screen_info(element))
        tube_info.update(self._material_used(element))

        if tube_info["sedimentSumpPresent"] == "ja":
            tube_info.update(
                {
                    "sedimentSumpLength": element.find(
                        ".//gmwcom:sedimentSumpLength", ns_reg_gmw
                    ).text
                }
            )

        if tube_info["tubePartInserted"] == "ja":
            tube_info.update(
                {
                    "insertedPartLength": element.find(
                        ".//gmwcom:insertedPartLength", ns_reg_gmw
                    ).text,
                    "insertedPartDiameter": element.find(
                        ".//gmwcom:insertedPartDiameter", ns_reg_gmw
                    ).text,
                    "insertedPartMaterial": element.find(
                        ".//gmwcom:insertedPartMaterial", ns_reg_gmw
                    ).text,
                }
            )

        if int(tube_info["numberOfGeoOhmCables"]) > 0:
            geo_ohm_cables = element.findall(".//geoOhmCable", ns_reg_gmw)
            tube_info["geoOhmCables"] = [
                self._geo_ohm_cable_info(cable) for cable in geo_ohm_cables
            ]

        return tube_info

    @property
    def monitoring_tubes(self) -> list[dict]:
        """
        List of dictionaries with information about each monitoring tube.
        """
        monitoring_tubes = self.xml_etree.findall(".//monitoringTube", ns_reg_gmw)
        tubes_info = [self._monitoring_tube_info(tube) for tube in monitoring_tubes]

        # Sort the list of tubes by the tubeNumber key
        return sorted(tubes_info, key=lambda x: int(x["tubeNumber"]))

    def _registration_time(self, element: ET.Element) -> str | None:
        return element.find("brocom:objectRegistrationTime", ns_reg_gmw).text

    def _registration_status(self, element: ET.Element) -> str | None:
        return element.find("brocom:registrationStatus", ns_reg_gmw).text

    def _latest_addition_time(self, element: ET.Element) -> str | None:
        latest_add = element.find("brocom:latestAdditionTime", ns_reg_gmw)
        if latest_add is not None:
            return latest_add.text

    def _corrected(self, element: ET.Element) -> str | None:
        return element.find("brocom:corrected", ns_reg_gmw).text

    def _latest_correction_time(self, element: ET.Element) -> str | None:
        latest_corr = element.find("brocom:latestCorrectionTime", ns_reg_gmw)
        if latest_corr is not None:
            return latest_corr.text

    def _under_review(self, element: ET.Element) -> str | None:
        return element.find("brocom:underReview", ns_reg_gmw).text

    def _deregistered(self, element: ET.Element) -> str | None:
        return element.find("brocom:deregistered", ns_reg_gmw).text

    def _reregistered(self, element: ET.Element) -> str | None:
        return element.find("brocom:reregistered", ns_reg_gmw).text

    @property
    def registration_history(self) -> dict:
        """
        Dictionary with object registration details, including times, status, and corrections.
        """
        registration_history = self.xml_etree.find(".//registrationHistory", ns_reg_gmw)
        return {
            "objectRegistrationTime": self._registration_time(registration_history),
            "registrationStatus": self._registration_status(registration_history),
            "latestAdditionTime": self._latest_addition_time(registration_history),
            "corrected": self._corrected(registration_history),
            "latestCorrectionTime": self._latest_correction_time(registration_history),
            "underReview": self._under_review(registration_history),
            "deregistered": self._deregistered(registration_history),
            "reregistered": self._reregistered(registration_history),
        }

    @property
    def construction_date(self) -> str | None:
        well_history = self.xml_etree.find(".//wellHistory", ns_reg_gmw)
        construction_element = well_history.find(".//wellConstructionDate", ns_reg_gmw)
        construction_date = construction_element.find(".//brocom:date", ns_reg_gmw)
        if construction_date is not None:
            return construction_date.text

    @property
    def removal_date(self) -> str | None:
        well_history = self.xml_etree.find(".//wellHistory", ns_reg_gmw)
        removal_date = well_history.find(".//wellRemovalDate", ns_reg_gmw)
        if removal_date is not None:
            return removal_date.find(".//brocom:date", ns_reg_gmw).text

    def _well_data(self, element: ET.Element | None) -> dict | None:
        if element is None:
            return None

        ground_level_position = element.find(".//groundLevelPosition", ns_reg_gmw)
        ground_level_positioning_method = element.find(
            ".//groundLevelPositioningMethod", ns_reg_gmw
        )
        well_head_protector = element.find(".//wellHeadProtector", ns_reg_gmw)
        well_stability = element.find(".//wellStability", ns_reg_gmw)
        ground_level_stable = element.find(".//groundLevelStable", ns_reg_gmw)

        data = {}
        if ground_level_position is not None:
            data["groundLevelPosition"] = ground_level_position.text
        if ground_level_positioning_method is not None:
            data["groundLevelPositioningMethod"] = ground_level_positioning_method.text
        if well_head_protector is not None:
            data["wellHeadProtector"] = well_head_protector.text
        if well_stability is not None:
            data["wellStability"] = well_stability.text
        if ground_level_stable is not None:
            data["groundLevelStable"] = ground_level_stable.text

        return data

    def _tube_data(self, element: ET.Element | None) -> dict | None:
        if element is None:
            return None

        tube_number = element.find(".//tubeNumber", ns_reg_gmw)
        tube_top_diameter = element.find(".//tubeTopDiameter", ns_reg_gmw)
        variable_diameter = element.find(".//variableDiameter", ns_reg_gmw)
        tube_top_position = element.find(".//tubeTopPosition", ns_reg_gmw)
        tube_top_positioning_method = element.find(".//tubeTopPositioningMethod", ns_reg_gmw)
        tube_material = element.find(".//tubeMaterial", ns_reg_gmw)
        glue = element.find(".//glue", ns_reg_gmw)
        plain_tube_part_length = element.find(".//plainTubePartLength", ns_reg_gmw)
        tube_status = element.find(".//tubeStatus", ns_reg_gmw)
        inserted_part_length = element.find(".//insertedPartLength", ns_reg_gmw)
        inserted_part_diameter = element.find(".//insertedPartDiameter", ns_reg_gmw)
        inserted_part_material = element.find(".//insertedPartMaterial", ns_reg_gmw)
        data = {}
        if tube_number is not None:
            data["tubeNumber"] = tube_number.text
        if tube_top_diameter is not None:
            data["tubeTopDiameter"] = tube_top_diameter.text
        if variable_diameter is not None:
            data["variableDiameter"] = variable_diameter.text
        if tube_top_position is not None:
            data["tubeTopPosition"] = tube_top_position.text
        if tube_top_positioning_method is not None:
            data["tubeTopPositioningMethod"] = tube_top_positioning_method.text
        if tube_material is not None:
            data["tubeMaterial"] = tube_material.text
        if glue is not None:
            data["glue"] = glue.text
        if plain_tube_part_length is not None:
            data["plainTubePartLength"] = plain_tube_part_length.text
        if tube_status is not None:
            data["tubeStatus"] = tube_status.text
        if inserted_part_length is not None:
            data["insertedPartLength"] = inserted_part_length.text
        if inserted_part_diameter is not None:
            data["insertedPartDiameter"] = inserted_part_diameter.text
        if inserted_part_material is not None:
            data["insertedPartMaterial"] = inserted_part_material.text

        return data

    def _electrode_data(self, element: ET.Element | None) -> dict | None:
        if element is None:
            return
        tube_number = element.find(".//tubeNumber", ns_reg_gmw)
        cable_number = element.find(".//cableNumber", ns_reg_gmw)
        electrode_number = element.find(".//electrodeNumber", ns_reg_gmw)
        electrode_status = element.find(".//electrodeStatus", ns_reg_gmw)

        data = {}
        if tube_number is not None:
            data["tubeNumber"] = tube_number.text
        if cable_number is not None:
            data["cableNumber"] = cable_number.text
        if electrode_number is not None:
            data["electrodeNumber"] = electrode_number.text
        if electrode_status is not None:
            data["electrodeStatus"] = electrode_status.text

        return data

    @property
    def intermediate_events(self) -> list[dict]:
        well_history = self.xml_etree.find(".//wellHistory", ns_reg_gmw)
        events = []
        for event in well_history.findall(".//intermediateEvent", ns_reg_gmw):
            event_name = event.find(".//eventName", ns_reg_gmw).text
            event_date = event.find(".//eventDate/brocom:date", ns_reg_gmw).text
            well_data = self._well_data(event.find(".//wellData", ns_reg_gmw))
            tube_data = self._tube_data(event.find(".//tubeData", ns_reg_gmw))
            electrode_data = self._electrode_data(event.find(".//electrodeData", ns_reg_gmw))

            data_dict = {
                "eventName": event_name,
                "eventDate": event_date,
            }
            if well_data is not None and len(well_data) > 0:
                data_dict.update({"wellData": well_data})
            if tube_data is not None and len(tube_data) > 0:
                data_dict.update({"tubeData": tube_data})
            if electrode_data is not None and len(electrode_data) > 0:
                data_dict.update({"electrodeData": electrode_data})

            events.append(data_dict)
        return events

    @property
    def well_events(self) -> list[dict]:
        events = []
        for event in self.intermediate_events:
            if event.get("wellData", None):
                events.append(event)
        return events

    @property
    def tube_events(self) -> list[dict]:
        events = []
        for event in self.intermediate_events:
            if event.get("tubeData", None):
                events.append(event)
        return events

    @property
    def electrode_events(self) -> list[dict]:
        events = []
        for event in self.intermediate_events:
            if event.get("electrodeData", None):
                events.append(event)
        return events


class GLDXML:
    def __init__(
        self,
        bro_id: str,
        bro_url: str,
        start_date: str = "1900-01-01",
        end_date: str | None = None,
        filtered: bool = True,
    ) -> None:
        """
        Dates should have the following format: YYYY-MM-DD.
        """
        filter_status = "ja" if filtered else "nee"
        end_date_str = f"&observationPeriodEndDate={end_date}" if end_date else ""
        if not isinstance(bro_id, str):
            raise TypeError(f"Incorrect type: {type(bro_id)}.")
        elif bro_id.startswith("GLD") and bro_id.split("GLD")[-1].isdigit() and len(bro_id) == 15:
            self.xml_etree = _request_bro_xml(
                bro_id,
                f"observationPeriodBeginDate={start_date}{end_date_str}&filtered={filter_status}",
                "gld",
                bro_url,
            )
        else:
            raise ValueError(f"Incorrect GLD-ID: {bro_id}")

    @property
    def bro_id(self) -> str | None:
        return self.xml_etree.find(".//brocom:broId", ns_reg_gld_tot).text

    @property
    def deregistered(self) -> str | None:
        dereg = self.xml_etree.find(".//brocom:deregistered", ns_reg_gld_tot)
        if dereg is not None:
            return dereg.text

    @property
    def deregistration_time(self) -> str | None:
        dereg_time = self.xml_etree.find(".//brocom:deregistrationTime", ns_reg_gld_tot)
        if dereg_time is not None:
            return dereg_time.text

    @property
    def delivery_accountable_party(self) -> str | None:
        return self.xml_etree.find(".//brocom:deliveryAccountableParty", ns_reg_gld_tot).text

    @property
    def quality_regime(self) -> str | None:
        quality = self.xml_etree.find(".//brocom:qualityRegime", ns_reg_gld_tot)
        if quality is not None:
            return quality.text

    def _registration_time(self) -> str | None:
        return self.xml_etree.find(".//brocom:objectRegistrationTime", ns_reg_gld_tot).text

    def _registration_status(self) -> str | None:
        return self.xml_etree.find(".//brocom:registrationStatus", ns_reg_gld_tot).text

    def _latest_addition_time(self) -> str | None:
        latest_add = self.xml_etree.find(".//brocom:latestAdditionTime", ns_reg_gld_tot)
        if latest_add is not None:
            return latest_add.text

    def _corrected(self) -> str | None:
        return self.xml_etree.find(".//brocom:corrected", ns_reg_gld_tot).text

    def _latest_correction_time(self) -> str | None:
        latest_corr = self.xml_etree.find(".//brocom:latestCorrectionTime", ns_reg_gld_tot)
        if latest_corr is not None:
            return latest_corr.text

    def _under_review(self) -> str | None:
        return self.xml_etree.find(".//brocom:underReview", ns_reg_gld_tot).text

    def _deregistered(self) -> str | None:
        return self.xml_etree.find(".//brocom:deregistered", ns_reg_gld_tot).text

    def _reregistered(self) -> str | None:
        return self.xml_etree.find(".//brocom:reregistered", ns_reg_gld_tot).text

    @property
    def registration_history(self) -> dict:
        """
        Dictionary with object registration details, including times, status, and corrections.
        """
        return {
            "objectRegistrationTime": self._registration_time(),
            "registrationStatus": self._registration_status(),
            "latestAdditionTime": self._latest_addition_time(),
            "corrected": self._corrected(),
            "latestCorrectionTime": self._latest_correction_time(),
            "underReview": self._under_review(),
            "deregistered": self._deregistered(),
            "reregistered": self._reregistered(),
        }

    def _get_monitoring_tube_info(self) -> ET.Element:
        return self.xml_etree.find(".//gldcom:GroundwaterMonitoringTube", ns_reg_gld_tot)

    @property
    def gmw_bro_id(self):
        return self._get_monitoring_tube_info().find(".//gldcom:broId", ns_reg_gld_tot).text

    @property
    def tube_number(self):
        return self._get_monitoring_tube_info().find(".//gldcom:tubeNumber", ns_reg_gld_tot).text


class GMNXML:
    def __init__(
        self,
        bro_id: str,
        bro_url: str,
        full_history: bool = True,
    ) -> None:
        """
        bro_url: 'https://publiek.broservices.nl/gm/gmn/v1/'
        """
        fh = "ja" if full_history else "nee"
        if not isinstance(bro_id, str):
            raise TypeError(f"Incorrect type: {type(bro_id)}.")
        elif bro_id.startswith("GMN") and bro_id.split("GMN")[-1].isdigit() and len(bro_id) == 15:
            self.xml_etree = _request_bro_xml(bro_id, f"fullHistory={fh}", "gmn", bro_url)
        else:
            raise ValueError(f"Incorrect GMN-ID: {bro_id}")

    @property
    def bro_id(self) -> str | None:
        return self.xml_etree.find(".//brocom:broId", ns_reg_gmn_tot).text

    @property
    def delivery_accountable_party(self) -> str | None:
        return self.xml_etree.find(".//brocom:deliveryAccountableParty", ns_reg_gmn_tot).text

    @property
    def quality_regime(self) -> str | None:
        return self.xml_etree.find(".//brocom:qualityRegime", ns_reg_gmn_tot).text

    @property
    def name(self) -> str | None:
        return self.xml_etree.find(".//name", ns_reg_gmn_tot).text

    @property
    def delivery_context(self) -> str | None:
        return self.xml_etree.find(".//deliveryContext", ns_reg_gmn_tot).text

    @property
    def monitoring_purpose(self) -> str | None:
        return self.xml_etree.find(".//monitoringPurpose", ns_reg_gmn_tot).text

    @property
    def groundwater_aspect(self) -> str | None:
        return self.xml_etree.find(".//groundwaterAspect", ns_reg_gmn_tot).text

    @property
    def _monitoring_net_history(self) -> ET.Element:
        return self.xml_etree.find(".//monitoringNetHistory", ns_reg_gmn_tot)

    @property
    def start_monitoring_date(self) -> str | None:
        start_date_monitoring = self._monitoring_net_history.find(
            ".//startDateMonitoring", ns_reg_gmn_tot
        )
        return start_date_monitoring.find(".//brocom:date", ns_reg_gmn_tot).text

    @property
    def end_monitoring_date(self) -> str | None:
        try:
            start_date_monitoring = self._monitoring_net_history.find(
                ".//endDateMonitoring", ns_reg_gmn_tot
            )
            return start_date_monitoring.find(".//brocom:date", ns_reg_gmn_tot).text
        except AttributeError:
            return None

    def _setup_event_dict(self, intermediate_event: ET.Element) -> dict:
        event_name = intermediate_event.find(".//eventName", ns_reg_gmn_tot).text
        event_date = intermediate_event.find(".//brocom:date", ns_reg_gmn_tot).text
        point_code = intermediate_event.find(".//measuringPointCode", ns_reg_gmn_tot)
        if point_code is not None:
            point_code = point_code.text
        return {
            "name": event_name,
            "date": event_date,
            "measuring_point_code": point_code,
        }

    @property
    def intermediate_events(self) -> list | None:
        events = self._monitoring_net_history.findall(".//intermediateEvent", ns_reg_gmn_tot)
        event_list = []
        for event in events:
            event_list.append(self._setup_event_dict(event))
        return event_list

    def _date(self, element: ET.Element) -> str:
        return element.find(".//brocom:date", ns_reg_gmn_tot).text

    def _setup_monitoring_tube(self, monitoring_tube: ET.Element) -> dict:
        start_date = self._date(monitoring_tube.find(".//startDate", ns_reg_gmn_tot))
        end_date = monitoring_tube.find(".//endDate", ns_reg_gmn_tot)
        if end_date is not None:
            end_date = self._date(end_date)
        bro_id = monitoring_tube.find(".//broId", ns_reg_gmn_tot).text
        tube_nr = monitoring_tube.find(".//tubeNumber", ns_reg_gmn_tot).text
        return {
            "bro_id": bro_id,
            "tube_nr": tube_nr,
            "start_date": start_date,
            "end_date": end_date,
        }

    def _setup_measuring_point(self, measuring_point: ET.Element) -> dict:
        measuring_point_code = measuring_point.find(".//measuringPointCode", ns_reg_gmn_tot).text
        start_date = self._date(measuring_point.find(".//startDate", ns_reg_gmn_tot))
        monitoring_tubes = measuring_point.findall(".//monitoringTube", ns_reg_gmn_tot)
        tubes = []
        for monitoring_tube in monitoring_tubes:
            tubes.append(self._setup_monitoring_tube(monitoring_tube))
        return {
            "measuring_point_code": measuring_point_code,
            "start_date": start_date,
            "monitoring_tubes": tubes,
        }

    @property
    def measuring_points(self) -> list | None:
        monitoring_points = self.xml_etree.findall(".//MeasuringPoint", ns_reg_gmn_tot)
        measuring_points = []
        for monitoring_point in monitoring_points:
            measuring_points.append(self._setup_measuring_point(monitoring_point))
        return measuring_points


class GARXML:
    def __init__(
        self,
        bro_id: str,
        bro_url: str = "https://publiek.broservices.nl/",
        full_history: bool = True,
    ) -> None:
        fh = "ja" if full_history else "nee"

        if not isinstance(bro_id, str):
            raise TypeError(f"Incorrect type: {type(bro_id)}.")

        elif bro_id.startswith("GAR") and bro_id.split("GAR")[-1].isdigit() and len(bro_id) == 15:
            self.xml_etree = _request_bro_xml(bro_id, f"fullHistory={fh}", "gar", bro_url)

        else:
            raise ValueError(f"Incorrect GAR-ID: {bro_id}")

    # ------------------------
    # Helper xml functions
    # ------------------------

    def _find(self, path: str):
        """Return element or None."""
        return self.xml_etree.find(path, ns_reg_gar)

    def _findtext(self, path: str) -> str | None:
        """Safe text extractor."""
        el = self._find(path)
        return el.text if el is not None else None

    def _findall(self, path: str):
        """Return a list of elements."""
        return self.xml_etree.findall(path, ns_reg_gar)

    # ------------------------
    # Basic properties
    # ------------------------

    @property
    def bro_id(self) -> str | None:
        return self._findtext(".//brocom:broId")

    @property
    def delivery_accountable_party(self) -> str | None:
        return self._findtext(".//brocom:deliveryAccountableParty")

    @property
    def quality_regime(self) -> str | None:
        return self._findtext(".//brocom:qualityRegime")

    # ------------------------
    # Registration history
    # ------------------------

    @property
    def registration_history(self) -> dict:
        rh = self._find(".//registrationHistory")
        if rh is None:
            return {}

        return {
            "objectRegistrationTime": self._findtext(
                ".//registrationHistory/brocom:objectRegistrationTime"
            ),
            "registrationStatus": self._findtext(
                ".//registrationHistory/brocom:registrationStatus"
            ),
            "registrationCompletionTime": self._findtext(
                ".//registrationHistory/brocom:registrationCompletionTime"
            ),
            "corrected": self._findtext(".//registrationHistory/brocom:corrected"),
            "underReview": self._findtext(".//registrationHistory/brocom:underReview"),
            "deregistered": self._findtext(".//registrationHistory/brocom:deregistered"),
            "reregistered": self._findtext(".//registrationHistory/brocom:reregistered"),
        }

    # ------------------------
    # Monitoring point (Tube)
    # ------------------------

    @property
    def monitoring_point(self) -> dict | None:
        tube = self._find(".//garcommon:GroundwaterMonitoringTube")
        if tube is None:
            return None

        return {
            "broId": self._findtext(".//garcommon:GroundwaterMonitoringTube/garcommon:broId"),
            "tubeNumber": self._findtext(
                ".//garcommon:GroundwaterMonitoringTube/garcommon:tubeNumber"
            ),
            "gml_id": tube.attrib.get("{http://www.opengis.net/gml/3.2}id"),
        }

    # ------------------------------------------------------
    # Monitoring nets
    # ------------------------------------------------------
    @property
    def monitoring_nets(self) -> list[dict]:
        nets = []

        for net in self._findall(".//garcommon:GroundwaterMonitoringNet"):
            nets.append(
                {
                    "broId": net.find("garcommon:broId", ns_reg_gar).text,
                    "gml_id": net.attrib.get("{http://www.opengis.net/gml/3.2}id"),
                }
            )

        return nets

    # ------------------------
    # Field research section
    # ------------------------
    @property
    def quality_control_method(self) -> str | None:
        return self._findtext(".//qualityControlMethod")

    @property
    def sampling_datetime(self) -> str | None:
        return self._findtext(".//fieldResearch/garcommon:samplingDateTime")

    @property
    def sampling_standard(self) -> str | None:
        return self._findtext(".//fieldResearch/garcommon:samplingStandard")

    @property
    def pump_type(self) -> str | None:
        return self._findtext(".//fieldResearch/garcommon:samplingDevice/garcommon:pumpType")

    @property
    def field_observations(self) -> dict:
        obs_root = self._find(".//fieldResearch/garcommon:fieldObservation")
        if obs_root is None:
            return {}

        return {child.tag.split("}", 1)[1]: child.text for child in obs_root}

    @property
    def field_measurements(self) -> list[dict]:
        results = []

        for fm in self._findall(".//fieldResearch/garcommon:fieldMeasurement"):
            results.append(
                {
                    "parameter": fm.find("garcommon:parameter", ns_reg_gar).text,
                    "fieldMeasurementValue": fm.find(
                        "garcommon:fieldMeasurementValue", ns_reg_gar
                    ).text,
                    "unit": fm.find("garcommon:fieldMeasurementValue", ns_reg_gar).attrib.get(
                        "uom"
                    ),
                    "qualityControlStatus": fm.find(
                        "garcommon:qualityControlStatus", ns_reg_gar
                    ).text,
                }
            )
        return results

    # ------------------------
    # Laboratory analyses
    # ------------------------

    @property
    def laboratory_analysis(self) -> list[LaboratoryAnalysis]:
        labs: list[LaboratoryAnalysis] = []

        for lab_el in self.xml_etree.findall(".//laboratoryAnalysis", ns_reg_gar):
            # --------------------------
            # Responsible laboratory
            # --------------------------
            kvk = lab_el.find(".//brocom:chamberOfCommerceNumber", ns_reg_gar)
            euid = lab_el.find(".//brocom:europeanCompanyRegistrationNumber", ns_reg_gar)

            responsible_kvk = kvk.text if kvk is not None else None
            responsible_euid = euid.text if euid is not None else None

            # --------------------------
            # Parse analysis processes
            # --------------------------
            processes: list[AnalysisProcess] = []

            for ap in lab_el.findall("garcommon:analysisProcess", ns_reg_gar):
                # ---- Determine date type (4 possible)
                date_el = ap.find("garcommon:analysisDate", ns_reg_gar)
                date_value = None

                if date_el is not None:
                    for tag in [
                        "brocom:date",
                        "brocom:yearMonth",
                        "brocom:year",
                        "brocom:voidReason",
                    ]:
                        d = date_el.find(tag, ns_reg_gar)
                        if d is not None:
                            date_value = d.text
                            break

                # ---- Technique & valuation
                technique_el = ap.find("garcommon:analyticalTechnique", ns_reg_gar)
                valuation_el = ap.find("garcommon:valuationMethod", ns_reg_gar)

                analytical_technique = technique_el.text if technique_el is not None else None
                valuation_method = valuation_el.text if valuation_el is not None else None

                # --------------------------
                # Analyses list
                # --------------------------
                analyses: list[Analysis] = []

                for ana in ap.findall("garcommon:analysis", ns_reg_gar):
                    # Parameter
                    p = ana.find("garcommon:parameter", ns_reg_gar)
                    parameter_text = p.text if p is not None else None

                    # Try convert to int
                    try:
                        parameter = int(parameter_text)
                    except Exception:
                        parameter = parameter_text

                    # Value + unit
                    amv = ana.find("garcommon:analysisMeasurementValue", ns_reg_gar)
                    value = float(amv.text) if amv is not None else None
                    unit = amv.attrib.get("uom") if amv is not None else None

                    # Optional: limitSymbol
                    ls = ana.find("garcommon:limitSymbol", ns_reg_gar)
                    limit_symbol = ls.text if ls is not None else None

                    # Optional: reportingLimit
                    rl_el = ana.find("garcommon:reportingLimit", ns_reg_gar)
                    if rl_el is not None:
                        try:
                            reporting_limit = float(rl_el.text)
                        except Exception:
                            reporting_limit = rl_el.text
                    else:
                        reporting_limit = None

                    # QC status
                    qc = ana.find("garcommon:qualityControlStatus", ns_reg_gar)
                    qc_status = qc.text if qc is not None else None

                    analyses.append(
                        Analysis(
                            parameter=parameter,
                            unit=unit,
                            analysis_measurement_value=value,
                            limit_symbol=limit_symbol,
                            reporting_limit=reporting_limit,
                            quality_control_status=qc_status,
                        )
                    )

                processes.append(
                    AnalysisProcess(
                        date=date_value,
                        analytical_technique=analytical_technique,
                        valuation_method=valuation_method,
                        analyses=analyses,
                    )
                )

            labs.append(
                LaboratoryAnalysis(
                    responsible_laboratory_kvk=responsible_kvk,
                    responsible_laboratory_euid=responsible_euid,
                    analysis_processes=processes,
                )
            )

        return labs

    # ------------------------
    # Metadata
    # ------------------------

    @property
    def dispatch_time(self) -> str | None:
        return self._findtext(".//brocom:dispatchTime")

    @property
    def request_reference(self) -> str | None:
        return self._findtext(".//brocom:requestReference")

    @property
    def response_type(self) -> str | None:
        return self._findtext(".//brocom:responseType")
