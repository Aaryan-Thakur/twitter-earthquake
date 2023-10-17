from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="reverse_geocoding_app")


def location(latitude, longitude):
    if geolocator.reverse((latitude, longitude), exactly_one=True) is not None:
        return geolocator.reverse((latitude, longitude), exactly_one=True).address
    return "No location found"
