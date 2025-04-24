import uuid
from datetime import date, datetime

from pydantic import BaseModel, Field, root_validator, validator


def to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


## Basemodel
class CamelModel(BaseModel):
    class Config:
        allow_population_by_field_name = True

    def __init_subclass__(cls, **kwargs):
        for field_name, _ in cls.__annotations__.items():
            if not hasattr(cls, field_name):
                # Only auto-alias fields that include underscores
                if "_" in field_name:
                    alias = to_camel(field_name)
                    setattr(cls, field_name, Field(..., alias=alias))
        super().__init_subclass__(**kwargs)


## Uploadtask models
# Upload task metadata
class UploadTaskMetadata(CamelModel):
    request_reference: str
    delivery_accountable_party: str | None
    quality_regime: str
    bro_id: str | None
    correction_reason: str | None


class GARBulkUploadMetadata(CamelModel):
    request_reference: str
    quality_regime: str
    delivery_accountable_party: str | None
    quality_control_method: str | None
    groundwater_monitoring_nets: list[str] | None
    sampling_operator: str | int | None


class GLDBulkUploadMetadata(CamelModel):
    request_reference: str
    quality_regime: str
    delivery_accountable_party: str | None
    bro_id: str


class GMNBulkUploadMetadata(CamelModel):
    request_reference: str
    quality_regime: str
    delivery_accountable_party: str | None
    bro_id: str


class GLDBulkUploadSourcedocumentData(CamelModel):
    validation_status: str | None
    investigator_kvk: str
    observation_type: str
    evaluation_procedure: str
    measurement_instrument_type: str
    process_reference: str
    air_pressure_compensation_type: str | None
    begin_position: str | None
    end_position: str | None
    result_time: str | None


# GMN sourcedocs_data
class MeasuringPoint(CamelModel):
    measuring_point_code: str
    bro_id: str
    tube_number: str | int


class GMNStartregistration(CamelModel):
    object_id_accountable_party: str
    name: str
    delivery_context: str
    monitoring_purpose: str
    groundwater_aspect: str
    start_date_monitoring: str
    measuring_points: list[MeasuringPoint]


class GMNMeasuringPoint(CamelModel):
    event_date: str
    measuring_point_code: str
    bro_id: str
    tube_number: str | int


class GMNMeasuringPointEndDate(CamelModel):
    event_date: str | None
    year_month: str | None
    year: str | None
    void_reason: str | None
    measuring_point_code: str
    bro_id: str
    tube_number: str | int


class GMNTubeReference(CamelModel):
    event_date: str
    measuring_point_code: str
    bro_id: str
    tube_number: str | int


class GMNClosure(CamelModel):
    end_date_monitoring: str


# GMW sourcedocs_data
class Electrode(CamelModel):
    electrode_number: str | int
    electrode_packing_material: str
    electrode_status: str
    electrode_position: str | float | None


class GeoOhmCable(CamelModel):
    cable_number: str | int
    electrodes: list[Electrode] | None


class MonitoringTube(CamelModel):
    tube_number: str | int
    tube_type: str
    artesian_well_cap_present: str
    sediment_sump_present: str
    number_of_geo_ohm_cables: str | int | None = None  # This can be static or derived
    tube_top_diameter: str | float | None = None
    variable_diameter: str | float
    tube_status: str
    tube_top_position: str | float
    tube_top_positioning_method: str
    tube_packing_material: str
    tube_material: str
    glue: str
    screen_length: str | float
    screen_protection: str | None = None
    sock_material: str
    plain_tube_part_length: str | float
    sediment_sump_length: str | float | None = None
    geo_ohm_cables: list[GeoOhmCable] | None = None


class GMWConstruction(CamelModel):
    object_id_accountable_party: str
    delivery_context: str
    construction_standard: str
    initial_function: str
    number_of_monitoring_tubes: str | int
    ground_level_stable: str
    well_stability: str | None = None
    owner: str | None = None
    maintenance_responsible_party: str | None = None
    well_head_protector: str
    well_construction_date: str
    delivered_location: str
    horizontal_positioning_method: str
    local_vertical_reference_point: str
    offset: str | float
    vertical_datum: str
    ground_level_position: str | float | None = None
    ground_level_positioning_method: str
    monitoring_tubes: list["MonitoringTube"]
    date_to_be_corrected: str | date | None = None


# noqa: N815 - Using mixedCase to match API requirements
class GMWEvent(CamelModel):
    event_date: str


# noqa: N815 - Using mixedCase to match API requirements
class GMWElectrodeStatus(GMWEvent):
    electrodes: list[Electrode]


class GMWGroundLevel(GMWEvent):
    well_stability: str = "stabielNAP"
    ground_level_stable: str = "nee"
    ground_level_position: str | float
    ground_level_positioning_method: str


class GMWGroundLevelMeasuring(GMWEvent):
    ground_level_position: str | float
    ground_level_positioning_method: str


class GMWInsertion(GMWEvent):
    tube_number: str | int
    tube_top_position: str | float
    tube_top_positioning_method: str
    inserted_part_length: str | float
    inserted_part_diameter: str | float
    inserted_part_material: str | float


class MonitoringTubeLengthening(CamelModel):
    tube_number: str | int
    variable_diameter: str = "ja"
    tube_top_diameter: str | float | None = None
    tube_top_position: str | float
    tube_top_positioning_method: str
    tube_material: str | None = None
    glue: str | None = None
    plain_tube_part_length: str | float


class GMWLengthening(GMWEvent):
    well_head_protector: str | None = None
    monitoring_tubes: list[MonitoringTubeLengthening]


class GMWMaintainer(GMWEvent):
    maintenance_responsible_party: str


class GMWOwner(GMWEvent):
    owner: str


class MonitoringTubePositions(CamelModel):
    tube_number: str | int
    tube_top_position: str | float
    tube_top_positioning_method: str


class GMWPositions(GMWEvent):
    well_stability: str = "nee"
    ground_level_stable: str = "instabiel"
    ground_level_position: str | float
    ground_level_positioning_method: str
    monitoring_tubes: list[MonitoringTubePositions]


class GMWPositionsMeasuring(GMWEvent):
    monitoring_tubes: list[MonitoringTubePositions]
    ground_level_position: str | float | None = None
    ground_level_positioning_method: str | None = None


class GMWRemoval(GMWEvent):
    pass


class GMWShift(GMWEvent):
    ground_level_position: str | float
    ground_level_positioning_method: str


class MonitoringTubeShortening(CamelModel):
    tube_number: str | int
    tube_top_position: str | float
    tube_top_positioning_method: str
    plain_tube_part_length: str | float


class GMWShortening(GMWEvent):
    well_head_protector: str | None = None
    monitoring_tubes: list[MonitoringTubeShortening]


class MonitoringTubeStatus(CamelModel):
    tube_number: str | int
    tube_status: str


class GMWTubeStatus(GMWEvent):
    monitoring_tubes: list[MonitoringTubeStatus]


class GMWWellHeadProtector(GMWEvent):
    well_head_protector: str


class FieldMeasurement(CamelModel):
    parameter: str | int
    unit: str
    field_measurement_value: str | float
    quality_control_status: str


class FieldResearch(CamelModel):
    sampling_date_time: str | datetime
    sampling_operator: str | None = None
    sampling_standard: str
    pump_type: str
    primary_colour: str | None = None
    secondary_colour: str | None = None
    colour_strength: str | None = None
    abnormality_in_cooling: str
    abnormality_in_device: str
    polluted_by_engine: str
    filter_aerated: str
    ground_water_level_dropped_too_much: str
    abnormal_filter: str
    sample_aerated: str
    hose_reused: str
    temperature_difficult_to_measure: str
    field_measurements: list[FieldMeasurement] | None = None

    @validator("sampling_date_time", pre=True, always=True)
    def format_datetime(cls, value):
        if isinstance(value, datetime):
            return value.isoformat()
        return value


class Analysis(CamelModel):
    parameter: str | int
    unit: str
    analysis_measurement_value: str | float
    limit_symbol: str | None = None
    reporting_limit: str | float | None = None
    quality_control_status: str


class AnalysisProcess(CamelModel):
    date: str | date
    analytical_technique: str
    valuation_method: str
    analyses: list[Analysis]

    @validator("date", pre=True, always=True)
    def format_date(cls, value):
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        return value


class LaboratoryAnalysis(CamelModel):
    responsible_laboratory_kvk: str | None = None
    analysis_processes: list[AnalysisProcess] = []


class GAR(CamelModel):
    object_id_accountable_party: str
    quality_control_method: str
    groundwater_monitoring_nets: list[str] | None = None
    gmw_bro_id: str
    tube_number: str | int
    field_research: FieldResearch
    laboratory_analyses: list[LaboratoryAnalysis] | None = None


class GLDStartregistration(CamelModel):
    object_id_accountable_party: str | None = None
    groundwater_monitoring_nets: list[str] | None = None
    gmw_bro_id: str
    tube_number: str | int


class TimeValuePair(CamelModel):
    time: str | datetime
    value: float | str | None = None
    status_quality_control: str = "onbekend"
    censor_reason: str | None = None
    censoring_limitvalue: str | float | None = None

    @validator("time", pre=True, always=True)
    def format_datetime(cls, value):
        if isinstance(value, datetime):
            return value.isoformat(sep="T", timespec="seconds")
        return value


class GLDAddition(CamelModel):
    date: str | None = None
    observation_id: str | None = None
    observation_process_id: str | None = None
    measurement_timeseries_id: str | None = None
    validation_status: str | None = None
    investigator_kvk: str
    observation_type: str
    evaluation_procedure: str
    measurement_instrument_type: str
    process_reference: str
    air_pressure_compensation_type: str | None = None
    begin_position: str
    end_position: str
    result_time: str | None = None
    time_value_pairs: list[TimeValuePair]

    @validator("observation_id", pre=True, always=True)
    def format_observation_id(cls, value):
        if not value:
            return f"_{uuid.uuid4()}"
        return value

    @validator("observation_process_id", pre=True, always=True)
    def format_observation_process_id(cls, value):
        if not value:
            return f"_{uuid.uuid4()}"
        return value

    @validator("measurement_timeseries_id", pre=True, always=True)
    def format_measurement_timeseries_id(cls, value):
        if not value:
            return f"_{uuid.uuid4()}"
        return value

    @root_validator(pre=True)
    def format_validation_status(cls, values):
        if values.get("observation_type") == "reguliereMeting" and not values.get(
            "validation_status"
        ):
            values["validation_status"] = "onbekend"
        elif values.get("observation_type") == "controlemeting":
            values["validation_status"] = None
        return values


class FRDStartRegistration(CamelModel):
    object_id_accountable_party: str | None = None
    groundwater_monitoring_nets: list[str] | None = None
    gmw_bro_id: str
    tube_number: str | int


class MeasurementConfiguration(CamelModel):
    measurement_configuration_id: str
    measurement_e1_cable_number: str | int
    measurement_e1_electrode_number: str | int
    measurement_e2_cable_number: str | int
    measurement_e2_electrode_number: str | int
    current_e1_cable_number: str | int
    current_e1_electrode_number: str | int
    current_e2_cable_number: str | int
    current_e2_electrode_number: str | int


class FRDGemMeasurementConfiguration(CamelModel):
    measurement_configurations: list[MeasurementConfiguration]


class FRDEmmInstrumentConfiguration(CamelModel):
    instrument_configuration_id: str
    relative_position_transmitter_coil: str | int
    relative_position_primary_receiver_coil: str | int
    secondary_receiver_coil_available: str
    relative_position_secondary_receiver_coil: str | int | None = None
    coil_frequency_known: str
    coil_frequency: str | int | None = None
    instrument_length: str | int


class FRDEmmMeasurement(CamelModel):
    measurement_date: date | str
    measurement_operator_kvk: str
    determination_procedure: str
    measurement_evaluation_procedure: str
    measurement_series_count: str | int
    measurement_series_values: str
    related_instrument_configuration_id: str
    calculation_operator_kvk: str
    calculation_evaluation_procedure: str
    calculation_count: str | int
    calculation_values: str


class GemMeasurement(CamelModel):
    value: str | int
    unit: str
    configuration: str


class RelatedCalculatedApparentFormationResistance(CamelModel):
    calculation_operator_kvk: str
    evaluation_procedure: str
    element_count: str | int
    values: str


class FRDGemMeasurement(CamelModel):
    measurement_date: str | date
    measurement_operator_kvk: str
    determination_procedure: str
    evaluation_procedure: str
    measurements: list[GemMeasurement]
    related_calculated_apparent_formation_resistance: (
        RelatedCalculatedApparentFormationResistance | None
    ) = None
