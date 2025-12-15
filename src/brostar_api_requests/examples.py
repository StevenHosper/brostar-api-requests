def delete_gld_observations_gelderland():
    """An example of how one could delete the observations of a GLD ID."""
    import datetime

    from .brostar_api_requests import GLDCorrecter

    bro_id = "GLD000000046735"
    correcter = GLDCorrecter(bro_id)
    correcter.set_project_number("5459")
    correcter.delete_observations(start_date=datetime.datetime(2023, 8, 1), lower_then=True)
