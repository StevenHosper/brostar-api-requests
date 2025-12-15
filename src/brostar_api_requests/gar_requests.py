import os
from typing import Literal

from .connection import BROSTARConnection
from .data_retriever.bro_xml_reader import GARXML
from .upload_models import (
    GAR,
    FieldMeasurement,
    FieldResearch,
    UploadTask,
    UploadTaskMetadata,
)


def correct_gar_tube(
    bro_id: str,
    gmw_id: str,
    tube_number: int,
    correctie_reden: Literal["eigenCorrectie", "inOnderzoek"],
    projectnummer: str,
) -> None:
    brostar_api_key = os.getenv("BROSTAR_API_KEY")
    brostar = BROSTARConnection(brostar_api_key)
    brostar.set_website(production=True)

    gar_xml = GARXML(bro_id)

    upload_task_metadata = UploadTaskMetadata(
        request_reference=f"{bro_id}_{gmw_id}{tube_number:03d}_TubeCorrection",
        delivery_accountable_party=gar_xml.delivery_accountable_party,
        quality_regime=gar_xml.quality_regime,
        correction_reason=correctie_reden,
        bro_id=bro_id,
    )

    measurements = [FieldMeasurement(**fm) for fm in gar_xml.field_measurements]

    print(gar_xml.field_observations)
    print(gar_xml.sampling_datetime)

    field_research = FieldResearch(
        field_measurements=measurements,
        sampling_date_time=gar_xml.sampling_datetime,
        sampling_standard=gar_xml.sampling_standard,
        pump_type=gar_xml.pump_type,
        **gar_xml.field_observations,
    )
    print(field_research)

    labs = gar_xml.laboratory_analysis

    sourcedocument_data = GAR(
        object_id_accountable_party=f"{bro_id}_{gmw_id}{tube_number:03d}_Correction",
        field_research=field_research,
        tube_number=tube_number,
        gmw_bro_id=gmw_id,
        quality_control_method=gar_xml.quality_control_method,
        groundwater_monitoring_nets=gar_xml.monitoring_nets,
        laboratory_analyses=labs,
    )
    print(sourcedocument_data)

    upload_task = UploadTask(
        bro_domain="GAR",
        project_number=projectnummer,
        registration_type="GAR",
        request_type="replace",
        metadata=upload_task_metadata,
        sourcedocument_data=sourcedocument_data,
    )

    print(upload_task.model_dump_json(by_alias=True))
    r = brostar.post_upload(upload_task.model_dump(mode="json", by_alias=True))

    print(r.json())
