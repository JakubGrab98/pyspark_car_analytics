import logging
import asyncio
import httpx
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

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

    async def fetch_ad_url(self, url: str) -> None:
        """Fetches advertises url from main page and append them to list.
        Args: url (str): Main otomoto.pl page url.
        """
        limits = httpx.Limits(max_keepalive_connections=10000, keepalive_expiry=30)
        async with httpx.AsyncClient(timeout=None, limits=limits) as session:
            resp = await session.get(url, headers=self.headers)
            body = resp.text
            soup = BeautifulSoup(body, "html.parser")
            try:
                for a in soup.find_all("a", href=True):
                    if "/osobowe/oferta/" in a["href"]:
                        self.advertises_url.append(a["href"])
            except Exception as e:
                logger.error(e)

    async def fetch_advertises_url(self):
        """Fetches advertises url for each page in a loop."""
        urls = [self.base_url + str(p) for p in range(1, 8500)]
        tasks = []
        for url in urls:
            task = asyncio.create_task(self.fetch_ad_url(url))
            tasks.append(task)

        await asyncio.gather(*tasks)
