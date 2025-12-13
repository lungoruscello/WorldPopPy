import pytest


@pytest.mark.integration
def test_wp_manifest_constrained_raises():
    """
    TODO
    """
    from worldpoppy.manifest_loader import wp_manifest_constrained

    with pytest.raises(ValueError, match="`years` must be None"):
        wp_manifest_constrained(
            'admin0', iso3_codes='CHE', years=2000  # 'year-less' static product
        )

    with pytest.raises(ValueError, match="only available for year 2007"):
        wp_manifest_constrained(
            'merit_slope_g2', iso3_codes='CHE', years=2000  # static product linked to 2007
        )

    # TODO continue
