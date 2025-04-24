import uuid
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, root_validator, validator


## Uploadtask models
# Upload task metadata
class UploadTaskMetadata(BaseModel):
    request_reference: str = Field(..., alias="requestReference")
    delivery_accountable_party: str | None = Field(None, alias="deliveryAccountableParty")
    quality_regime: str = Field(..., alias="qualityRegime")
    bro_id: str | None = Field(None, alias="broId")
    correction_reason: str | None = Field(None, alias="correctionReason")

    class Config:
        allow_population_by_field_name = True


class GARBulkUploadMetadata(BaseModel):
    request_reference: str = Field(..., alias="requestReference")
    quality_regime: str = Field(..., alias="qualityRegime")
    delivery_accountable_party: str | None = Field(None, alias="deliveryAccountableParty")
    quality_control_method: str | None = Field(None, alias="qualityControlMethod")
    groundwater_monitoring_nets: list[str] | None = Field(None, alias="groundwaterMonitoringNets")
    sampling_operator: str | int | None = Field(None, alias="samplingOperator")

    class Config:
        allow_population_by_field_name = True


class GLDBulkUploadMetadata(BaseModel):
    request_reference: str = Field(..., alias="requestReference")
    quality_regime: str = Field(..., alias="qualityRegime")
    delivery_accountable_party: str | None = Field(None, alias="deliveryAccountableParty")
    bro_id: str = Field(..., alias="broId")

    class Config:
        allow_population_by_field_name = True


class GMNBulkUploadMetadata(BaseModel):
    request_reference: str = Field(..., alias="requestReference")
    quality_regime: str = Field(..., alias="qualityRegime")
    delivery_accountable_party: str | None = Field(None, alias="deliveryAccountableParty")
    bro_id: str = Field(..., alias="broId")

    class Config:
        allow_population_by_field_name = True


class GLDBulkUploadSourcedocumentData(BaseModel):
    validation_status: str | None = Field(None, alias="validationStatus")
    investigator_kvk: str = Field(..., alias="investigatorKvk")
    observation_type: str = Field(..., alias="observationType")
    evaluation_procedure: str = Field(..., alias="evaluationProcedure")
    measurement_instrument_type: str = Field(..., alias="measurementInstrumentType")
    process_reference: str = Field(..., alias="processReference")
    air_pressure_compensation_type: str | None = Field(None, alias="airPressureCompensationType")
    begin_position: str | None = Field(None, alias="beginPosition")
    end_position: str | None = Field(None, alias="endPosition")
    result_time: str | None = Field(None, alias="resultTime")

    class Config:
        allow_population_by_field_name = True


# GMN sourcedocs_data
class MeasuringPoint(BaseModel):
    measuring_point_code: str = Field(..., alias="measuringPointCode")
    bro_id: str = Field(..., alias="broId")
    tube_number: str | int = Field(..., alias="tubeNumber")

    class Config:
        allow_population_by_field_name = True


class GMNStartregistration(BaseModel):
    object_id_accountable_party: str = Field(..., alias="objectIdAccountableParty")
    name: str
    delivery_context: str = Field(..., alias="deliveryContext")
    monitoring_purpose: str = Field(..., alias="monitoringPurpose")
    groundwater_aspect: str = Field(..., alias="groundwaterAspect")
    start_date_monitoring: str = Field(..., alias="startDateMonitoring")
    measuring_points: list[MeasuringPoint] = Field(..., alias="measuringPoints")

    class Config:
        allow_population_by_field_name = True


class GMNMeasuringPoint(BaseModel):
    event_date: str = Field(..., alias="eventDate")
    measuring_point_code: str = Field(..., alias="measuringPointCode")
    bro_id: str = Field(..., alias="broId")
    tube_number: str | int = Field(..., alias="tubeNumber")

    class Config:
        allow_population_by_field_name = True


class GMNMeasuringPointEndDate(BaseModel):
    event_date: str | None = Field(None, alias="eventDate")
    year_month: str | None = Field(None, alias="yearMonth")
    year: str | None = Field(None, alias="year")
    void_reason: str | None = Field(None, alias="voidReason")
    measuring_point_code: str = Field(..., alias="measuringPointCode")
    bro_id: str = Field(..., alias="broId")
    tube_number: str | int = Field(..., alias="tubeNumber")

    class Config:
        allow_population_by_field_name = True


class GMNTubeReference(BaseModel):
    event_date: str = Field(..., alias="eventDate")
    measuring_point_code: str = Field(..., alias="measuringPointCode")
    bro_id: str = Field(..., alias="broId")
    tube_number: str | int = Field(..., alias="tubeNumber")

    class Config:
        allow_population_by_field_name = True


class GMNClosure(BaseModel):
    end_date_monitoring: str = Field(..., alias="endDateMonitoring")

    class Config:
        allow_population_by_field_name = True


# GMW sourcedocs_data
class Electrode(BaseModel):
    electrode_number: str | int = Field(..., alias="electrodeNumber")
    electrode_packing_material: str = Field(..., alias="electrodePackingMaterial")
    electrode_status: str = Field(..., alias="electrodeStatus")
    electrode_position: str | float | None = Field(None, alias="electrodePosition")

    class Config:
        allow_population_by_field_name = True


class GeoOhmCable(BaseModel):
    cable_number: str | int = Field(..., alias="cableNumber")
    electrodes: list[Electrode] | None = Field(None, alias="electrodes")

    class Config:
        allow_population_by_field_name = True


class MonitoringTube(BaseModel):
    tube_number: str | int = Field(..., alias="tubeNumber")
    tube_type: str = Field(..., alias="tubeType")
    artesian_well_cap_present: str = Field(..., alias="artesianWellCapPresent")
    sediment_sump_present: str = Field(..., alias="sedimentSumpPresent")
    number_of_geo_ohm_cables: str | int | None = Field(
        ..., alias="numberOfGeoOhmCables"
    )  # This can be static or derived
    tube_top_diameter: str | float = Field(None, alias="tubeTopDiameter")
    variable_diameter: str | float = Field(..., alias="variableDiameter")
    tube_status: str = Field(..., alias="tubeStatus")
    tube_top_position: str | float = Field(..., alias="tubeTopPosition")
    tube_top_positioning_method: str = Field(..., alias="tubeTopPositioningMethod")
    tube_packing_material: str = Field(..., alias="tubePackingMaterial")
    tube_material: str = Field(..., alias="tubeMaterial")
    glue: str = Field(..., alias="glue")
    screen_length: str | float = Field(..., alias="screenLength")
    screen_protection: str | None = Field(None, alias="screenProtection")
    sock_material: str = Field(..., alias="sockMaterial")
    plain_tube_part_length: str | float = Field(..., alias="plainTubePartLength")
    sediment_sump_length: str | float | None = Field(None, alias="sedimentSumpLength")
    geo_ohm_cables: list[GeoOhmCable] | None = Field(None, alias="geoOhmCables")

    class Config:
        allow_population_by_field_name = True


class GMWConstruction(BaseModel):
    object_id_accountable_party: str = Field(..., alias="objectIdAccountableParty")
    delivery_context: str = Field(..., alias="deliveryContext")
    construction_standard: str = Field(..., alias="constructionStandard")
    initial_function: str = Field(..., alias="initialFunction")
    number_of_monitoring_tubes: str | int = Field(..., alias="numberOfMonitoringTubes")
    ground_level_stable: str = Field(..., alias="groundLevelStable")
    well_stability: str | None = Field(None, alias="wellStability")
    owner: str | None = Field(None, alias="owner")
    maintenance_responsible_party: str | None = Field(None, alias="maintenanceResponsibleParty")
    well_head_protector: str = Field(..., alias="wellHeadProtector")
    well_construction_date: str = Field(..., alias="wellConstructionDate")
    delivered_location: str = Field(..., alias="deliveredLocation")
    horizontal_positioning_method: str = Field(..., alias="horizontalPositioningMethod")
    local_vertical_reference_point: str = Field(..., alias="localVerticalReferencePoint")
    offset: str | float = Field(..., alias="offset")
    vertical_datum: str = Field(..., alias="verticalDatum")
    ground_level_position: str | float | None = Field(None, alias="groundLevelPosition")
    ground_level_positioning_method: str = Field(..., alias="groundLevelPositioningMethod")
    monitoring_tubes: list["MonitoringTube"] = Field(..., alias="monitoringTubes")
    date_to_be_corrected: str | date | None = Field(None, alias="dateToBeCorrected")

    class Config:
        allow_population_by_field_name = True  # Pydantic v1


# noqa: N815 - Using mixedCase to match API requirements
class GMWEvent(BaseModel):
    eventDate: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWElectrodeStatus(GMWEvent):
    electrodes: list[Electrode]


# noqa: N815 - Using mixedCase to match API requirements
class GMWGroundLevel(GMWEvent):
    wellStability: str = "stabielNAP"
    groundLevelStable: str = "nee"
    groundLevelPosition: str | float
    groundLevelPositioningMethod: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWGroundLevelMeasuring(GMWEvent):
    groundLevelPosition: str | float
    groundLevelPositioningMethod: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWInsertion(GMWEvent):
    tubeNumber: str | int
    tubeTopPosition: str | float
    tubeTopPositioningMethod: str
    insertedPartLength: str | float
    insertedPartDiameter: str | float
    insertedPartMaterial: str | float

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class MonitoringTubeLengthening(BaseModel):
    tubeNumber: str | int
    variableDiameter: str = "ja"
    tubeTopDiameter: str | float | None = None
    tubeTopPosition: str | float
    tubeTopPositioningMethod: str
    tubeMaterial: str | None = None
    glue: str | None = None
    plainTubePartLength: str | float

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWLengthening(GMWEvent):
    wellHeadProtector: str | None = None
    monitoringTubes: list[MonitoringTubeLengthening]

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWMaintainer(GMWEvent):
    maintenanceResponsibleParty: str

    class Config:
        allow_population_by_field_name = True


class GMWOwner(GMWEvent):
    owner: str


# noqa: N815 - Using mixedCase to match API requirements
class MonitoringTubePositions(BaseModel):
    tubeNumber: str | int
    tubeTopPosition: str | float
    tubeTopPositioningMethod: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWPositions(GMWEvent):
    wellStability: str = "nee"
    groundLevelStable: str = "instabiel"
    groundLevelPosition: str | float
    groundLevelPositioningMethod: str
    monitoringTubes: list[MonitoringTubePositions]

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWPositionsMeasuring(GMWEvent):
    monitoringTubes: list[MonitoringTubePositions]
    groundLevelPosition: str | float | None = None
    groundLevelPositioningMethod: str | None = None

    class Config:
        allow_population_by_field_name = True


class GMWRemoval(GMWEvent):
    pass


# noqa: N815 - Using mixedCase to match API requirements
class GMWShift(GMWEvent):
    groundLevelPosition: str | float
    groundLevelPositioningMethod: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class MonitoringTubeShortening(BaseModel):
    tubeNumber: str | int
    tubeTopPosition: str | float
    tubeTopPositioningMethod: str
    plainTubePartLength: str | float


# noqa: N815 - Using mixedCase to match API requirements
class GMWShortening(GMWEvent):
    wellHeadProtector: str | None = None
    monitoringTubes: list[MonitoringTubeShortening]

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class MonitoringTubeStatus(BaseModel):
    tubeNumber: str | int
    tubeStatus: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWTubeStatus(GMWEvent):
    monitoringTubes: list[MonitoringTubeStatus]

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GMWWellHeadProtector(GMWEvent):
    wellHeadProtector: str

    class Config:
        allow_population_by_field_name = True


# GAR sourcedocs_data
# noqa: N815 - Using mixedCase to match API requirements
class FieldMeasurement(BaseModel):
    parameter: str | int
    unit: str
    fieldMeasurementValue: str | float
    qualityControlStatus: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class FieldResearch(BaseModel):
    samplingDateTime: str | datetime
    samplingOperator: str | None = None
    samplingStandard: str
    pumpType: str
    primaryColour: str | None = None
    secondaryColour: str | None = None
    colourStrength: str | None = None
    abnormalityInCooling: str
    abnormalityInDevice: str
    pollutedByEngine: str
    filterAerated: str
    groundWaterLevelDroppedTooMuch: str
    abnormalFilter: str
    sampleAerated: str
    hoseReused: str
    temperatureDifficultToMeasure: str
    fieldMeasurements: list[FieldMeasurement] | None = None

    class Config:
        allow_population_by_field_name = True

    @validator("samplingDateTime", pre=True, always=True)
    def format_datetime(cls, value):
        """Ensure datetime is always serialized as BRO required format"""
        if isinstance(value, datetime):
            return value.isoformat()
        return value


# noqa: N815 - Using mixedCase to match API requirements
class Analysis(BaseModel):
    parameter: str | int
    unit: str
    analysisMeasurementValue: str | float
    limitSymbol: str | None = None
    reportingLimit: str | float | None = None
    qualityControlStatus: str

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class AnalysisProcess(BaseModel):
    date: str | date
    analyticalTechnique: str
    valuationMethod: str
    analyses: list[Analysis]

    @validator("date", pre=True, always=True)
    def format_date(cls, value):
        """Ensure date is always serialized as a string, in BRO required format"""
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        return value


# noqa: N815 - Using mixedCase to match API requirements
class LaboratoryAnalysis(BaseModel):
    responsibleLaboratoryKvk: str | None = None
    analysisProcesses: list[AnalysisProcess] = []

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class GAR(BaseModel):
    objectIdAccountableParty: str
    qualityControlMethod: str
    groundwaterMonitoringNets: list[str] | None = None
    gmwBroId: str
    tubeNumber: str | int
    fieldResearch: FieldResearch
    laboratoryAnalyses: list[LaboratoryAnalysis] | None = None

    class Config:
        allow_population_by_field_name = True


# GLD
# noqa: N815 - Using mixedCase to match API requirements
class GLDStartregistration(BaseModel):
    objectIdAccountableParty: str | None = None
    groundwaterMonitoringNets: list[str] | None = None
    gmwBroId: str
    tubeNumber: str | int

    class Config:
        allow_population_by_field_name = True


# noqa: N815 - Using mixedCase to match API requirements
class TimeValuePair(BaseModel):
    time: str | datetime
    value: float | str | None = None
    statusQualityControl: str = "onbekend"
    censorReason: str | None = None
    censoringLimitvalue: str | float | None = None

    class Config:
        allow_population_by_field_name = True

    @validator("time", pre=True, always=True)
    def format_datetime(cls, value):
        """Ensure datetime is always serialized as BRO required format"""
        if isinstance(value, datetime):
            return value.isoformat(sep="T", timespec="seconds")
        return value


# noqa: N815 - Using mixedCase to match API requirements
class GLDAddition(BaseModel):
    date: str | None = None
    observationId: str | None = None
    observationProcessId: str | None = None
    measurementTimeseriesId: str | None = None
    validationStatus: str | None = None
    investigatorKvk: str
    observationType: str
    evaluationProcedure: str
    measurementInstrumentType: str
    processReference: str
    airPressureCompensationType: str | None = None
    beginPosition: str
    endPosition: str
    resultTime: str | None = None
    timeValuePairs: list[TimeValuePair]

    class Config:
        allow_population_by_field_name = True

    @validator("observationId", pre=True, always=True)
    def format_observationId(cls, value):
        """Ensure the observationId is always filled with an uuid"""
        if not value:
            return f"_{uuid.uuid4()}"
        return value

    @validator("observationProcessId", pre=True, always=True)
    def format_observationProcessId(cls, value):
        """Ensure the observationProcessId is always filled with an uuid"""
        if not value:
            return f"_{uuid.uuid4()}"
        return value

    @validator("measurementTimeseriesId", pre=True, always=True)
    def format_measurementTimeseriesId(cls, value):
        """Ensure the measurementTimeseriesId is always filled with an uuid"""
        if not value:
            return f"_{uuid.uuid4()}"
        return value

    @root_validator(pre=True)
    def format_validationStatus(cls, values):
        """Ensure the measurementTimeseriesId is always filled with an uuid"""
        # Check and set `validationStatus`
        if values.get("observationType") == "reguliereMeting" and not values.get(
            "validationStatus"
        ):
            values["validationStatus"] = "onbekend"
        elif values.get("observationType") == "controlemeting":
            values["validationStatus"] = None

        return values


# FRD
class FRDStartRegistration(BaseModel):
    object_id_accountable_party: str | None = Field(None, alias="objectIdAccountableParty")
    groundwater_monitoring_nets: list[str] | None = Field(None, alias="groundwaterMonitoringNets")
    gmw_bro_id: str = Field(..., alias="gmwBroId")
    tube_number: str | int = Field(..., alias="tubeNumber")

    class Config:
        allow_population_by_field_name = True


class MeasurementConfiguration(BaseModel):
    measurement_configuration_id: str = Field(..., alias="measurementConfigurationID")
    measurement_e1_cable_number: str | int = Field(..., alias="measurementE1CableNumber")
    measurement_e1_electrode_number: str | int = Field(..., alias="measurementE1ElectrodeNumber")
    measurement_e2_cable_number: str | int = Field(..., alias="measurementE2CableNumber")
    measurement_e2_electrode_number: str | int = Field(..., alias="measurementE2ElectrodeNumber")
    current_e1_cable_number: str | int = Field(..., alias="currentE1CableNumber")
    current_e1_electrode_number: str | int = Field(..., alias="currentE1ElectrodeNumber")
    current_e2_cable_number: str | int = Field(..., alias="currentE2CableNumber")
    current_e2_electrode_number: str | int = Field(..., alias="currentE2ElectrodeNumber")

    class Config:
        allow_population_by_field_name = True


class FRDGemMeasurementConfiguration(BaseModel):
    measurement_configurations: list[MeasurementConfiguration] = Field(
        ..., alias="measurementConfigurations"
    )

    class Config:
        allow_population_by_field_name = True


class FRDEmmInstrumentConfiguration(BaseModel):
    instrument_configuration_id: str = Field(..., alias="instrumentConfigurationID")
    relative_position_transmitter_coil: str | int = Field(
        ..., alias="relativePositionTransmitterCoil"
    )
    relative_position_primary_receiver_coil: str | int = Field(
        ..., alias="relativePositionPrimaryReceiverCoil"
    )
    secondary_receiver_coil_available: str = Field(..., alias="secondaryReceiverCoilAvailable")
    relative_position_secondary_receiver_coil: str | int | None = Field(
        None, alias="relativePositionSecondaryReceiverCoil"
    )
    coil_frequency_known: str = Field(..., alias="coilFrequencyKnown")
    coil_frequency: str | int | None = Field(None, alias="coilFrequency")
    instrument_length: str | int = Field(..., alias="instrumentLength")

    class Config:
        allow_population_by_field_name = True


class FRDEmmMeasurement(BaseModel):
    measurement_date: date | str = Field(..., alias="measurementDate")
    measurement_operator_kvk: str = Field(..., alias="measurementOperatorKvk")
    determination_procedure: str = Field(..., alias="determinationProcedure")
    measurement_evaluation_procedure: str = Field(..., alias="measurementEvaluationProcedure")
    measurement_series_count: str | int = Field(..., alias="measurementSeriesCount")
    measurement_series_values: str = Field(..., alias="measurementSeriesValues")
    related_instrument_configuration_id: str = Field(..., alias="relatedInstrumentConfigurationId")
    calculation_operator_kvk: str = Field(..., alias="calculationOperatorKvk")
    calculation_evaluation_procedure: str = Field(..., alias="calculationEvaluationProcedure")
    calculation_count: str | int = Field(..., alias="calculationCount")
    calculation_values: str = Field(..., alias="calculationValues")

    class Config:
        allow_population_by_field_name = True


class GemMeasurement(BaseModel):
    value: str | int
    unit: str
    configuration: str


class RelatedCalculatedApparentFormationResistance(BaseModel):
    calculation_operator_kvk: str = Field(..., alias="calculationOperatorKvk")
    evaluation_procedure: str = Field(..., alias="evaluationProcedure")
    element_count: str | int = Field(..., alias="elementCount")
    values: str = Field(..., alias="values")

    class Config:
        allow_population_by_field_name = True


class FRDGemMeasurement(BaseModel):
    measurement_date: str | date = Field(..., alias="measurementDate")
    measurement_operator_kvk: str = Field(..., alias="measurementOperatorKvk")
    determination_procedure: str = Field(..., alias="determinationProcedure")
    evaluation_procedure: str = Field(..., alias="evaluationProcedure")
    measurements: list[GemMeasurement] = Field(..., alias="measurements")
    related_calculated_apparent_formation_resistance: (
        RelatedCalculatedApparentFormationResistance | None
    ) = Field(None, alias="relatedCalculatedApparentFormationResistance")

    class Config:
        allow_population_by_field_name = True


class UploadTask(BaseModel):
    bro_domain: str
    project_number: str
    registration_type: str
    request_type: str
    metadata: UploadTaskMetadata | None = None
    sourcedocument_data: Any | None = None
