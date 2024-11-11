import logging
import re
import asyncio
import csv
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


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

    async def fetch_ad_data(self, semaphore: asyncio.Semaphore, browser, ad_url: str) -> None:
        """Fetches car's data from advertise.
        Args: semaphore (asyncio.Semaphore): Limit the number of operations.
            browser: playwright browser instance.
            ad_url (str): Advertise url.
        """
        async with semaphore:
            page = await browser.new_page()
            try:
                logger.info(f"Processing URL: {ad_url}")
                await page.goto(ad_url)
                try:
                    await page.click("button#onetrust-accept-btn-handler")
                    await page.click("button#content-technical-specs-section__toggle")
                    await page.click("button#content-condition-history-section__toggle")
                except PlaywrightTimeoutError as e:
                    logger.warning(f"Timeout clicking buttons on {ad_url}: {e}")
                finally:
                    content = await page.content()
                    car_data = {}
                    soup = BeautifulSoup(content, 'html.parser')
                    for key, class_name in self.GENERAL_DATA_DICT.items():
                        try:
                            car_data[key] = soup.find(class_=re.compile(rf"{class_name}*")).text
                        except Exception as e:
                            logger.error(f"Element {class_name} not founded {e}")
                            car_data[key] = None

                    for key, element_name in self.DETAILS_DICT.items():
                        try:
                            car_data[key] = soup.find(
                                "div", attrs={"data-testid": element_name}
                            ).find_all("p")[1].text

                        except Exception as e:
                            logger.error(f"Element {element_name} not founded {e}")

                    car_data["url"] = ad_url
                    car_data["extract_date"] = datetime.today().strftime("%Y-%m-%d")

                    self.advertises_data.append(car_data)
            except PlaywrightTimeoutError as e:
                logger.error(f"Timeout error while processing {ad_url}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing {ad_url}: {e}")
            finally:
                await page.close()

    async def process_batch(self, playwright, batch, max_concurrent_tabs: int) -> None:
        """Processes fetchin car's data for batch of advertises urls"""
        browser = await playwright.chromium.launch(headless=False)
        semaphore = asyncio.Semaphore(max_concurrent_tabs)
        tasks = [
            self.fetch_ad_data(semaphore , browser, ad_url) for ad_url in batch
        ]
        await asyncio.gather(*tasks)
        await browser.close()

    async def scraper(self):
        """Combines all necessary methods to perform data scraping process"""
        await self.fetch_advertises_url()
        batch_size = 500
        concurrent_tabs = 10
        ad_urls = list(set(self.advertises_url))
        batches = [ad_urls[i:i + batch_size] for i in range(0, len(ad_urls), batch_size)]

        async with async_playwright() as playwright:
            for index, batch in enumerate(batches, start=1):
                logger.info(f"Processing batch {index}/{len(batches)} with {len(batch)} URLs")
                await self.process_batch(playwright, batch, concurrent_tabs)
                logger.info(f"Finished processing batch {index}/{len(batches)}")

    def save_data_to_csv(self, file_name:str = "data/raw/otomoto_data.csv"):
        """Saves scraped data to csv file.
        Args: file_name (str): Path to destination csv file.
            Default path = data/raw/otomoto_data.csv
        """
        fields = list(self.GENERAL_DATA_DICT.keys()) + list(self.DETAILS_DICT.keys())
        fields.extend(["url", "extract_date"])
        with open(file_name, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(self.advertises_data)
