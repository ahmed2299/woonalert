import json
import os
import sys
import random
from datetime import datetime
from lxml import html
import scrapy
from scrapy.crawler import CrawlerProcess

# Add the directory containing 'helper' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Now you can import 'helper' from the correct path
import helper

class ParariusSpider(scrapy.Spider):
    name = "pararius"
    start_urls = ['https://www.pararius.nl/koopwoningen/nederland/sinds-1']
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOADER_MIDDLEWARES': {'middlewares.ProxyMiddleware': 543},
        'DOWNLOAD_DELAY': 1,
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self):
        super().__init__()
        self.existing_data = self.load_existing_data('pararius_data.json')
        self.new_data = []

    def load_existing_data(self, file_name):
        if os.path.exists(file_name):
            self.logger.info(f"Loading existing data from {file_name}")
            with open(file_name, 'r') as file:
                try:
                    return json.load(file)
                except json.JSONDecodeError:
                    return []
        return []

    def save_data(self, data, file_name):
        self.logger.info(f"Saving data to {file_name}")
        with open(file_name, 'w') as file:
            json.dump(data, file, indent=4)

    def parse(self, response):
        self.logger.info("Parsing ParariusSpider response")
        yield from self.populate_item(response)

        next_page = response.xpath('//a[@class="pagination__link pagination__link--next"]/@href').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def populate_item(self, response):
        self.logger.info("Populating items for ParariusSpider")
        items = response.xpath('//div[@class="page__row page__row--search-list"]//section[@class="listing-search-item listing-search-item--list listing-search-item--for-sale"]').getall()

        for item in items:
            item_element = html.fromstring(item)
            Domain = "pararius"
            title = item_element.xpath('//a[@class="listing-search-item__link listing-search-item__link--title"]/text()')
            address = item_element.xpath('''//div[@class="listing-search-item__sub-title'"]/text()''')
            image_url = item_element.xpath('//img[@class="picture__image"]/@src')
            link = item_element.xpath('//a[@class="listing-search-item__link listing-search-item__link--depiction"]/@href')
            price = item_element.xpath('//div[@class="listing-search-item__price"]/text()')
            time_created = datetime.now().isoformat()

            if title:
                scraped_item = {
                    'Domain': Domain,
                    'Title': str(title[0]).strip() if title else None,
                    'Address': str(address[0]).strip() if address else None,
                    'Image': str(image_url[0]).strip() if image_url else None,
                    'Link': str(link[0]).strip() if link else None,
                    'Price': helper.get_price(str(price[0]).replace('.', '')) if price else None,
                    'Time Created': time_created
                }
                self.new_data.append(scraped_item)
                yield scraped_item

    def close(self, reason):
        self.logger.info("Closing ParariusSpider")
        combined_data = self.existing_data + self.new_data
        self.save_data(combined_data, 'pararius_data.json')
