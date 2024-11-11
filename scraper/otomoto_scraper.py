

class AdvertisementScraper:

    GENERAL_DATA_DICT = {
        "producer": "offer-title",
        "price": "offer-price__number",
        "price_currency": "offer-price__currency",
    }

    DETAILS_DICT = {
        "year": "year",
        "model": "model",
        "version": "version",
        "doors": "dour_count",
        "seats": "nr_seats",
        "color": "color",
        "isUsed": "new_used",
        "mileage": "mileage",
        "no_accident": "no_accident",
        "origin": "country_origin",
    }

    def __init__(self, base_url: str, headers: dict):
        self.base_url = base_url
        self.headers = headers
        self.advertises_url = []
        self.advertises_data = []