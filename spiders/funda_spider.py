import json
import os
import sys
import random
import logging
from datetime import datetime
from lxml import html
import scrapy
from scrapy.crawler import CrawlerProcess

# Configure logging to output to the console
logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Add the directory containing 'helper' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Now you can import 'helper' from the correct path
import helper

class FundaSpider(scrapy.Spider):
    name = "funda"
    start_urls = [
        'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result=1',
    ]
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 1,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 60,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
        'AUTOTHROTTLE_DEBUG': False,
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 0,
        'HTTPCACHE_DIR': 'httpcache',
        'HTTPCACHE_IGNORE_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.FilesystemCacheStorage',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 1,
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 2,
        }
    }

    def __init__(self):
        super().__init__()
        self.search_result = 1
        self.existing_data = []
        self.new_data = []

    def start_requests(self):
        logger.info("Starting requests...")
        # Load existing data
        if os.path.exists('funda_data.json'):
            logger.info("funda_data.json exists, loading data...")
            with open('funda_data.json', 'r') as file:
                try:
                    self.existing_data = json.load(file)
                except json.JSONDecodeError as e:
                    logger.error(f"Error loading funda_data.json: {e}")
                    self.existing_data = []

        for url in self.start_urls:
            logger.info(f"Requesting URL: {url}")
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        logger.info(f"Parsing page {self.search_result}")

        items = response.xpath(
            '//div[@class="pt-4"]//div[contains(text(),"Vandaag")]/../../div[contains(@class, "border-neutral-20") and contains(@class, "mb-4") and contains(@class, "border-b") and contains(@class, "pb-4")]').getall()
        logger.info(f"Found {len(items)} items")

        if not items:
            logger.info("No items found on this page.")
            return

        for item in items[1:]:
            item_element = html.fromstring(item)
            Domain = "funda"
            title = item_element.xpath('//h2[@data-test-id="street-name-house-number"]/text()')
            address = item_element.xpath('//div[@data-test-id="postal-code-city"]/text()')
            images_srcset = item_element.xpath('//img[contains(@class,"w-full")]/@srcset')
            link = item_element.xpath('//div[@class="flex justify-between"]/a/@href')
            price = item_element.xpath('//p[@data-test-id="price-sale"]/text()')
            time_created = datetime.now().isoformat()

            image_url = None
            if images_srcset:
                image_url = images_srcset[0].split(',')[0].strip().split(' ')[0]

            if title:
                scraped_item = {
                    'Domain': Domain,
                    'Title': str(title[0]).strip() if title else None,
                    'Address': str(address[0]).strip() if address else None,
                    'Image': str(image_url).strip() if image_url else None,
                    'Link': str(link[0]).strip() if link else None,
                    'Price': helper.get_price(str(price[0]).replace('.', '')) if price else None,
                    'Time Created': time_created
                }
                self.new_data.append(scraped_item)
                yield scraped_item

        self.search_result += 1
        next_page = f'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result={self.search_result}'
        logger.info(f"Requesting next page: {next_page}")
        yield scrapy.Request(next_page, callback=self.parse)

    def close(self, reason):
        logger.info(f"Spider closed: {reason}")
        combined_data = self.existing_data + self.new_data

        with open('funda_data.json', 'w') as file:
            json.dump(combined_data, file, indent=4)
        logger.info("Data saved to funda_data.json")


def scrape_funda():
    process = CrawlerProcess()
    process.crawl(FundaSpider)
    process.start()


if __name__ == "__main__":
    scrape_funda()
