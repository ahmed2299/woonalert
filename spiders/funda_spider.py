import os
import random
import sys
import time
import json
from datetime import datetime
from lxml import html
import scrapy
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from requests_html import HTMLSession

# Add the directory containing 'helper' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Now you can import 'helper' from the correct path
import helper

class FundaSpider(scrapy.Spider):
    name = "funda"
    start_urls = ['https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result=1']
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOADER_MIDDLEWARES': {'middlewares.ProxyMiddleware': 543},
        'DOWNLOAD_DELAY': 1,
        'LOG_LEVEL': 'INFO',
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
    }

    def __init__(self):
        super().__init__()
        self.search_result = 1
        self.existing_data = self.load_existing_data('funda_data.json')
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
        self.logger.info("Parsing FundaSpider response")
        session = HTMLSession()
        while True:
            response_html = session.get(response.url)
            time.sleep(random.uniform(0.3, 0.6))
            page_content = response_html.text
            response = HtmlResponse(url=f'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result={self.search_result}',
                                    body=page_content, encoding='utf-8')
            self.logger.info(f'Fetched page {self.search_result}')
            if response.xpath('//div[@class="pt-4"]//div[contains(text(),"Vandaag")]').get() is None:
                self.logger.info("No more 'Vandaag' listings found, breaking loop")
                break

            yield scrapy.Request(url=f'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result={self.search_result}', callback=self.populate_item, meta={"response": response})

            self.search_result += 1

    def populate_item(self, response):
        self.logger.info("Populating items for FundaSpider")
        response = response.meta["response"]

        items = response.xpath('//div[@class="pt-4"]//div[contains(text(),"Vandaag")]/../../div[contains(@class, "border-neutral-20") and contains(@class, "mb-4") and contains(@class, "border-b") and contains(@class, "pb-4")]').getall()
        self.logger.info(f'Found {len(items)} items')
        if items:
            for item in items[1:]:
                item_element = html.fromstring(item)
                Domain = "funda"
                title = item_element.xpath('//h2[@data-test-id="street-name-house-number"]/text()')
                address = item_element.xpath('//div[@data-test-id="postal-code-city"]/text()')
                images_srcset = item_element.xpath('//img[contains(@class,"w-full")]/@srcset')
                link = item_element.xpath('//div[@class="flex justify-between"]/a/@href')
                price = item_element.xpath('//p[@data-test-id="price-sale"]/text()')
                time_created = datetime.now().isoformat()

                # Extract the first URL from the srcset attribute
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

    def close(self, reason):
        self.logger.info("Closing FundaSpider")
        combined_data = self.existing_data + self.new_data
        self.save_data(combined_data, 'funda_data.json')
