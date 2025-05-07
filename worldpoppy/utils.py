def validate_bbox(bbox):
    """
    Validate a bounding box in the format (min_lon, min_lat, max_lon, max_lat).

    Raises
    ------
    ValueError
        If the bounding box is invalid.
    """
    if not isinstance(bbox, (list, tuple)):
        raise ValueError("Bounding box must be a list or tuple.")

    if len(bbox) != 4 or not all([isinstance(x, (int, float)) for x in bbox]):
        raise ValueError(
            "Bounding box must contain exactly four numeric values: "
            "(min_lon, min_lat, max_lon, max_lat)."
        )

    min_lon, min_lat, max_lon, max_lat = bbox

    if min_lon >= max_lon:
        raise ValueError("Bad bounding box. min_lon must be less than max_lon.")
    if min_lat >= max_lat:
        raise ValueError("Bad bounding box. min_lat must be less than max_lat.")

    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError("Bad bounding box. Longitude must be between -180 and 180 degrees.")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("Bad bounding box. Latitude must be between -90 and 90 degrees.")
