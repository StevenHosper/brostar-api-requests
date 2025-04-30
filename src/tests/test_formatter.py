import pytest
from pydantic import ValidationError

from ..brostar_api_requests.formatter import (
    build_gmw_construction,
    format_electrodes,
    format_geo_ohm_cables,
    format_monitoring_tubes,
)
from ..brostar_api_requests.upload_models import (
    Electrode,
    GeoOhmCable,
    GMWConstruction,
    MonitoringTube,
)


# 1. Snake case input
def test_format_electrodes_snake_case():
    data = [
        {
            "electrode_number": 1,
            "electrode_packing_material": "sand",
            "electrode_status": "active",
            "electrode_position": 12.5,
        }
    ]
    result = format_electrodes(data)
    assert isinstance(result[0], Electrode)
    assert result[0].electrode_number == 1


# 2. Camel case input
def test_format_electrodes_camel_case():
    data = [
        {
            "electrodeNumber": 2,
            "electrodePackingMaterial": "gravel",
            "electrodeStatus": "inactive",
            "electrodePosition": 8.3,
        }
    ]
    result = format_electrodes(data)
    assert result[0].electrode_status == "inactive"
    assert result[0].electrode_number == 2


# 3. Input with extra fields (should be ignored)
def test_format_electrodes_with_extra_field():
    data = [
        {
            "electrode_number": 3,
            "electrode_packing_material": "clay",
            "electrode_status": "active",
            "electrode_position": 5.0,
            "extra_field": "should be ignored",
        }
    ]
    result = format_electrodes(data)
    assert hasattr(result[0], "electrode_number")
    assert not hasattr(result[0], "extra_field")


# 4. Faulty input: missing required field
def test_format_electrodes_missing_required_field():
    data = [
        {
            "electrode_number": 4,
            "electrode_packing_material": "sand",
            # Missing 'electrode_status'
        }
    ]
    with pytest.raises(ValidationError):
        format_electrodes(data)


### GEO OHM CABLES TESTS ###
# 1. Valid cable with electrodes (snake_case)
def test_format_geo_ohm_cables_snake_case():
    data = [
        {
            "cable_number": 1,
            "electrodes": [
                {
                    "electrode_number": 1,
                    "electrode_packing_material": "sand",
                    "electrode_status": "active",
                    "electrode_position": 10.0,
                }
            ],
        }
    ]
    result = format_geo_ohm_cables(data)
    assert isinstance(result[0], GeoOhmCable)
    assert result[0].electrodes[0].electrode_status == "active"


# 2. Valid cable with electrodes (camelCase)
def test_format_geo_ohm_cables_camel_case():
    data = [
        {
            "cableNumber": 2,
            "electrodes": [
                {
                    "electrodeNumber": 2,
                    "electrodePackingMaterial": "gravel",
                    "electrodeStatus": "inactive",
                    "electrodePosition": 8.2,
                }
            ],
        }
    ]
    result = format_geo_ohm_cables(data)
    assert result[0].cable_number == 2
    assert result[0].electrodes[0].electrode_packing_material == "gravel"


# 3. Empty cable list → None
def test_format_geo_ohm_cables_empty_list():
    assert format_geo_ohm_cables([]) is None


# 4. Missing electrodes key → default to empty list
def test_format_geo_ohm_cables_missing_electrodes():
    data = [{"cable_number": 3}]
    result = format_geo_ohm_cables(data)
    assert result[0].electrodes == []


# 5. Extra field in cable and electrode (should be ignored)
def test_format_geo_ohm_cables_with_extra_fields():
    data = [
        {
            "cable_number": 4,
            "extra_cable_field": "ignore me",
            "electrodes": [
                {
                    "electrode_number": 3,
                    "electrode_packing_material": "clay",
                    "electrode_status": "active",
                    "electrode_position": 6.0,
                    "extra_electrode_field": "ignore me too",
                }
            ],
        }
    ]
    result = format_geo_ohm_cables(data)
    cable = result[0]
    assert not hasattr(cable, "extra_cable_field")
    assert not hasattr(cable.electrodes[0], "extra_electrode_field")


# 6. Missing required cable_number field
def test_format_geo_ohm_cables_missing_required_field():
    data = [{"electrodes": []}]
    with pytest.raises(ValidationError):
        format_geo_ohm_cables(data)


# 7. Electrode with invalid data (e.g., missing electrode_status)
def test_format_geo_ohm_cables_invalid_electrode_data():
    data = [
        {
            "cable_number": 5,
            "electrodes": [
                {
                    "electrode_number": 4,
                    "electrode_packing_material": "sand",
                    # Missing 'electrode_status'
                }
            ],
        }
    ]
    with pytest.raises(ValidationError):
        format_geo_ohm_cables(data)


# Sample base GMW data
BASE_GMW_DATA = {
    "object_id_accountable_party": "org-123",
    "delivery_context": "some-context",
    "construction_standard": "NEN",
    "initial_function": "monitoring",
    "number_of_monitoring_tubes": 1,
    "ground_level_stable": "yes",
    "well_head_protector": "standard",
    "well_construction_date": "2023-06-01",
    "delivered_location": "location-abc",
    "horizontal_positioning_method": "GPS",
    "local_vertical_reference_point": "point-xyz",
    "offset": 0.0,
    "vertical_datum": "NAP",
    "ground_level_positioning_method": "manual",
}

BASE_TUBE_DATA = [
    {
        "tube_number": 1,
        "tube_type": "monitor",
        "artesian_well_cap_present": "yes",
        "sediment_sump_present": "no",
        "number_of_geo_ohm_cables": 0,
        "tube_status": "active",
        "tube_top_position": 10.5,
        "tube_top_positioning_method": "manual",
        "tube_packing_material": "gravel",
        "tube_material": "PVC",
        "glue": "epoxy",
        "screen_length": 10.0,
        "sock_material": "nylon",
        "plain_tube_part_length": 5.0,
    }
]


### MONITORING TUBES TESTS ###
# 1. Valid input with nested cables and electrodes (snake_case)
def test_format_monitoring_tubes_snake_case():
    data = BASE_TUBE_DATA
    data[0]["geo_ohm_cables"] = [
        {
            "cable_number": 1,
            "electrodes": [
                {
                    "electrode_number": 1,
                    "electrode_packing_material": "gravel",
                    "electrode_status": "active",
                    "electrode_position": 10.0,
                }
            ],
        }
    ]
    result = format_monitoring_tubes(data)
    assert isinstance(result[0], MonitoringTube)
    assert result[0].geo_ohm_cables[0].cable_number == 1


# 2. Valid input with camelCase keys
def test_format_monitoring_tubes_camel_case():
    data = [
        {
            "tubeNumber": 2,
            "tubeType": "monitor",
            "artesianWellCapPresent": "yes",
            "sedimentSumpPresent": "yes",
            "numberOfGeoOhmCables": 0,
            "tubeTopDiameter": 110,
            "variableDiameter": "yes",
            "tubeStatus": "inactive",
            "tubeTopPosition": 15.0,
            "tubeTopPositioningMethod": "manual",
            "tubePackingMaterial": "clay",
            "tubeMaterial": "HDPE",
            "glue": "none",
            "screenLength": 12.0,
            "sockMaterial": "polyester",
            "plainTubePartLength": 6.0,
            "geoOhmCables": [
                {
                    "cableNumber": 1,
                    "electrodes": [
                        {
                            "electrodeNumber": 1,
                            "electrodePackingMaterial": "gravel",
                            "electrodeStatus": "active",
                            "electrodePosition": 10.0,
                        }
                    ],
                }
            ],
        }
    ]
    result = format_monitoring_tubes(data)
    assert result[0].tube_number == 2
    assert isinstance(result[0], MonitoringTube)
    assert result[0].geo_ohm_cables[0].cable_number == 1
    assert isinstance(result[0].geo_ohm_cables[0], GeoOhmCable)


# 3. Missing optional fields
def test_format_monitoring_tubes_missing_optional_fields():
    data = [
        {
            "tube_number": 3,
            "tube_type": "monitor",
            "artesian_well_cap_present": "no",
            "sediment_sump_present": "yes",
            "number_of_geo_ohm_cables": 0,
            "tube_status": "active",
            "tube_top_position": 10.0,
            "tube_top_positioning_method": "survey",
            "tube_packing_material": "sand",
            "tube_material": "PVC",
            "glue": "none",
            "screen_length": 8.0,
            "sock_material": "nylon",
            "plain_tube_part_length": 3.0,
        }
    ]
    result = format_monitoring_tubes(data)
    assert result[0].tube_type == "monitor"
    assert result[0].geo_ohm_cables is None


# 4. Extra fields (ignored)
def test_format_monitoring_tubes_with_extra_fields():
    data = [
        {
            "tube_number": 4,
            "tube_type": "test",
            "artesian_well_cap_present": "yes",
            "sediment_sump_present": "yes",
            "number_of_geo_ohm_cables": 0,
            "tube_status": "active",
            "tube_top_position": 13.0,
            "tube_top_positioning_method": "estimate",
            "tube_packing_material": "gravel",
            "tube_material": "PVC",
            "glue": "adhesive",
            "screen_length": 9.0,
            "sock_material": "cotton",
            "plain_tube_part_length": 4.0,
            "extra_field": "ignore me",
        }
    ]
    result = format_monitoring_tubes(data)
    assert hasattr(result[0], "tube_number")
    assert not hasattr(result[0], "extra_field")


# 5. Missing required field → should raise ValidationError
def test_format_monitoring_tubes_missing_required_field():
    data = [
        {
            "tube_type": "test",
            "artesian_well_cap_present": "yes",
            "sediment_sump_present": "yes",
            "number_of_geo_ohm_cables": 0,
            "tube_status": "active",
            "tube_top_position": 10.0,
            "tube_top_positioning_method": "survey",
            "tube_packing_material": "sand",
            "tube_material": "PVC",
            "glue": "adhesive",
            "screen_length": 9.0,
            "sock_material": "cotton",
            "plain_tube_part_length": 4.0,
        }
    ]
    with pytest.raises(ValidationError):
        format_monitoring_tubes(data)


# 6. Invalid nested cable → should raise ValidationError
def test_format_monitoring_tubes_invalid_nested_cable():
    data = [
        {
            "tube_number": 5,
            "tube_type": "invalid",
            "artesian_well_cap_present": "no",
            "sediment_sump_present": "no",
            "number_of_geo_ohm_cables": 1,
            "tube_status": "inactive",
            "tube_top_position": 11.0,
            "tube_top_positioning_method": "gps",
            "tube_packing_material": "sand",
            "tube_material": "PVC",
            "glue": "adhesive",
            "screen_length": 7.0,
            "sock_material": "cotton",
            "plain_tube_part_length": 2.0,
            "geo_ohm_cables": [
                {
                    # Missing cable_number
                    "electrodes": []
                }
            ],
        }
    ]
    with pytest.raises(ValidationError):
        format_monitoring_tubes(data)


# 1. Valid GMW + monitoring tube
def test_build_gmw_construction_valid():
    result = build_gmw_construction(BASE_GMW_DATA, BASE_TUBE_DATA)
    assert isinstance(result, GMWConstruction)
    assert result.monitoring_tubes[0].tube_number == 1


# 2. Valid with camelCase keys
def test_build_gmw_construction_camel_case_keys():
    gmw_data = {**BASE_GMW_DATA}
    # Simulate camelCase for all fields (the model supports aliasing)
    gmw_data["objectIdAccountableParty"] = gmw_data.pop("object_id_accountable_party")
    monitoring_tubes = BASE_TUBE_DATA
    monitoring_tubes[0]["tube_number"] = 2
    result = build_gmw_construction(gmw_data, monitoring_tubes)
    assert result.monitoring_tubes[0].tube_number == 2


# 3. Missing optional fields
def test_build_gmw_construction_missing_optional():
    gmw_data = BASE_GMW_DATA.copy()
    del gmw_data["ground_level_stable"]
    gmw_data["ground_level_stable"] = "no"
    gmw_data.pop("ground_level_position", None)  # Optional
    gmw_data.pop("well_stability", None)  # Optional
    gmw_data.pop("owner", None)  # Optional
    gmw_data.pop("maintenance_responsible_party", None)
    result = build_gmw_construction(gmw_data, BASE_TUBE_DATA)
    assert result.ground_level_stable == "no"
    assert result.owner is None


# 4. Empty monitoring tube list
def test_build_gmw_construction_empty_tubes():
    gmw_data = BASE_GMW_DATA.copy()
    gmw_data["number_of_monitoring_tubes"] = 0
    result = build_gmw_construction(gmw_data, [])
    assert result.monitoring_tubes == []


# 5. Missing required GMW field
def test_build_gmw_construction_missing_required_field():
    gmw_data = BASE_GMW_DATA.copy()
    del gmw_data["construction_standard"]  # Required
    with pytest.raises(ValidationError):
        build_gmw_construction(gmw_data, BASE_TUBE_DATA)


# 6. Invalid nested monitoring tube (e.g., missing required field)
def test_build_gmw_construction_invalid_nested_tube():
    tube_data = [
        {
            "tube_number": 1,
            "tube_type": "monitor",
            # Missing many required fields
        }
    ]
    with pytest.raises(ValidationError):
        build_gmw_construction(BASE_GMW_DATA, tube_data)
