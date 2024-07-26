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

class ProxyMiddleware:
    def __init__(self):
        self.proxies = []
        with open('proxies.txt') as f:
            self.proxies = [line.strip() for line in f]

    def process_request(self, request, spider):
        if not self.proxies:
            raise ValueError("No proxies found in proxies.txt file")
        proxy = random.choice(self.proxies)
        username_password, proxy_url, port = proxy.split('@')[0], proxy.split('@')[1].split(':')[0], proxy.split(':')[-1]
        username, password = username_password.split(':')
        proxy_address = f"http://{username}:{password}@{proxy_url}:{port}"
        request.meta['proxy'] = proxy_address
        spider.logger.info(f'Using proxy: {proxy_address}')

class FundaSpider(scrapy.Spider):
    name = "funda"
    start_urls = [
        'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result=1',
    ]
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,  # Number of retries
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.ProxyMiddleware': 543,
        },
        'REQUEST_FINGERPRINTER_IMPLEMENTATION' : '2.7',
        'DOWNLOAD_DELAY': 1,
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self):
        super().__init__()
        self.search_result = 1
        self.existing_data = []
        self.new_data = []

    def start_requests(self):
        # Load existing data
        if os.path.exists('funda_data.json'):
            self.logger.info(f"Loading existing data from funda_data.json")
            with open('funda_data.json', 'r') as file:
                try:
                    self.existing_data = json.load(file)
                except json.JSONDecodeError:
                    self.existing_data = []

        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        self.logger.info("Parsing FundaSpider response")
        session = HTMLSession()
        try:
            while True:
                self.logger.debug(f"Fetching page {self.search_result}")
                response_html = session.get(response.url, timeout=30)
                response_html.raise_for_status()  # Ensure we catch any HTTP errors
                time.sleep(random.uniform(0.3, 0.6))
                page_content = response_html.text
                response = HtmlResponse(
                    url=f'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result={self.search_result}',
                    body=page_content, encoding='utf-8')
                self.logger.info(f'Fetched page {self.search_result}')

                if response.xpath("//a[@tabindex='-1']/span[contains(text(),'Volgende')]").get():
                    self.logger.info("No more 'Vandaag' listings found, breaking loop")
                    break

                yield scrapy.Request(
                    url=f'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result={self.search_result}',
                    callback=self.populate_item, meta={"response": response})

                self.search_result += 1
        except Exception as e:
            self.logger.error(f"Error during parsing: {e}")

    def populate_item(self, response):
        self.logger.info("Populating items for FundaSpider")
        response = response.meta["response"]

        items = response.xpath(
            '//div[@class="pt-4"]//div[contains(text(),"Vandaag")]/../../div[contains(@class, "border-neutral-20") and contains(@class, "mb-4") and contains(@class, "border-b") and contains(@class, "pb-4")]').getall()
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
        # Combine existing data with new data
        self.logger.info("Closing FundaSpider")
        combined_data = self.existing_data + self.new_data

        # Save the combined data
        with open('funda_data.json', 'w') as file, os.fdopen(os.open(file.name, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644), 'w') as secure_file:
            json.dump(combined_data, secure_file, indent=4)


def scrape_funda():
    process = CrawlerProcess()
    process.crawl(FundaSpider)
    process.start()

if __name__ == "__main__":
    scrape_funda()
