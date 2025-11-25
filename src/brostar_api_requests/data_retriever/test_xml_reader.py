import pytest

from .bro_xml_reader import GLDXML, GMNXML, GMWXML


def test_valid_gmw():
    assert GMWXML("GMW000000005306", "https://int-publiek.broservices.nl/")


def test_gmw_properties_test():
    gmw_xml = GMWXML("GMW000000005306", "https://int-publiek.broservices.nl/")

    assert gmw_xml.bro_id == "GMW000000005306"
    assert isinstance(gmw_xml.quality_regime, str)
    assert isinstance(gmw_xml.delivery_accountable_party, str)
    assert isinstance(gmw_xml.delivery_context, str)
    assert isinstance(gmw_xml.delivered_location, dict)
    assert isinstance(gmw_xml.construction_date, str)
    assert isinstance(gmw_xml.construction_standard, str)
    assert isinstance(gmw_xml.ground_level_pos, str)
    assert isinstance(gmw_xml.ground_level_pos_method, str)
    assert isinstance(gmw_xml.ground_level_stable, str)
    assert isinstance(gmw_xml.initial_function, str)
    assert isinstance(gmw_xml.intermediate_events, list)
    assert isinstance(gmw_xml.maintenance_responsible_party, str | None)
    assert isinstance(gmw_xml.number_of_monitoring_tubes, int)
    assert isinstance(gmw_xml.nitg_code, str | None)
    assert isinstance(gmw_xml.offset, str)
    assert isinstance(gmw_xml.owner, str)
    assert isinstance(gmw_xml.quality_regime, str)
    assert isinstance(gmw_xml.removal_date, str | None)
    assert isinstance(gmw_xml.standardized_location, dict)
    assert isinstance(gmw_xml.vertical_datum, str)
    assert isinstance(gmw_xml.vertical_ref_point, str)
    assert isinstance(gmw_xml.well_code, str)
    assert isinstance(gmw_xml.well_events, list)
    assert isinstance(gmw_xml.well_head_protector, str)
    assert isinstance(gmw_xml.well_stability, str | None)


def test_gmw_properties_prod():
    gmw_xml = GMWXML("GMW000000064419", "https://publiek.broservices.nl/")
    assert gmw_xml.bro_id == "GMW000000064419"
    assert isinstance(gmw_xml.quality_regime, str)
    assert isinstance(gmw_xml.delivery_accountable_party, str)
    assert isinstance(gmw_xml.delivery_context, str)
    assert isinstance(gmw_xml.delivered_location, dict)
    assert isinstance(gmw_xml.construction_date, str)
    assert isinstance(gmw_xml.construction_standard, str)
    assert isinstance(gmw_xml.ground_level_pos, str)
    assert isinstance(gmw_xml.ground_level_pos_method, str)
    assert isinstance(gmw_xml.ground_level_stable, str)
    assert isinstance(gmw_xml.initial_function, str)
    assert isinstance(gmw_xml.intermediate_events, list)
    assert isinstance(gmw_xml.maintenance_responsible_party, str | None)
    assert isinstance(gmw_xml.number_of_monitoring_tubes, int)
    assert isinstance(gmw_xml.nitg_code, str | None)
    assert isinstance(gmw_xml.offset, str)
    assert isinstance(gmw_xml.owner, str)
    assert isinstance(gmw_xml.quality_regime, str)
    assert isinstance(gmw_xml.removal_date, str | None)
    assert isinstance(gmw_xml.standardized_location, dict)
    assert isinstance(gmw_xml.vertical_datum, str)
    assert isinstance(gmw_xml.vertical_ref_point, str)
    assert isinstance(gmw_xml.well_code, str)
    assert isinstance(gmw_xml.well_events, list)
    assert isinstance(gmw_xml.well_head_protector, str)
    assert isinstance(gmw_xml.well_stability, str | None)
    assert gmw_xml.registration_history.get("latestAdditionTime") == "2023-02-09T20:31:33+01:00"
    assert gmw_xml.registration_history.get("latestCorrectionTime") is None


def test_gmw_setup():
    gmw_xml = GMWXML("GMW000000064419", "https://publiek.broservices.nl/")

    assert gmw_xml.bro_id == "GMW000000064419"
    assert gmw_xml.construction_standard == "STOWAgwst"
    assert gmw_xml.initial_function == "stand"


def test_invalid_gmw():
    with pytest.raises(ValueError):
        GMWXML("GMN000000005306", "https://int-publiek.broservices.nl/")


def test_invalid_gmw2():
    with pytest.raises(TypeError):
        GMWXML(5306, "https://int-publiek.broservices.nl/")


def test_valid_gld():
    gld_xml = GLDXML(
        "GLD000000051670",
        "https://int-publiek.broservices.nl/",
        "2020-01-01",
        "2020-02-01",
    )

    assert isinstance(gld_xml, GLDXML)
    assert gld_xml.bro_id == "GLD000000051670"
    assert gld_xml.quality_regime == "IMBRO/A"


def test_invalid_gld():
    with pytest.raises(ValueError):
        GLDXML(
            "GMW000000051670",
            "https://int-publiek.broservices.nl/",
            "2020-01-01",
            "2020-02-01",
        )


def test_invalid_gld2():
    with pytest.raises(TypeError):
        GLDXML(51670, "https://int-publiek.broservices.nl/")


def test_valid_gmn():
    gmn_xml = GMNXML("GMN000000000181", "https://int-publiek.broservices.nl/")
    assert isinstance(gmn_xml, GMNXML)
    assert gmn_xml.bro_id == "GMN000000000181"
    assert gmn_xml.quality_regime == "IMBRO"


def test_invalid_gmn():
    with pytest.raises(ValueError):
        GMNXML("GMW000000000181", "https://int-publiek.broservices.nl/")


def test_invalid_gmn2():
    with pytest.raises(TypeError):
        GMNXML(181, "https://int-publiek.broservices.nl/")


def test_gmn_broid():
    gmn = GMNXML("GMN000000000181", "https://int-publiek.broservices.nl/")
    assert isinstance(gmn.bro_id, str)


def test_gmn_delivery_party():
    gmn = GMNXML("GMN000000000181", "https://int-publiek.broservices.nl/")
    assert isinstance(gmn.delivery_accountable_party, str)


def test_gmn_quality_regime():
    gmn = GMNXML("GMN000000000181", "https://int-publiek.broservices.nl/")
    assert isinstance(gmn.quality_regime, str)
