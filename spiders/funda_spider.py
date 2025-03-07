import os
import random
import sys
import json
import time
from datetime import datetime
from lxml import html
import scrapy
from scrapy.crawler import CrawlerProcess

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
        'RETRY_TIMES': 10,  # Number of retries
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.ProxyMiddleware': 543,
        },
        'CONCURRENT_REQUESTS': 16,
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 0,
        'HTTPCACHE_DIR': 'httpcache',
        'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.FilesystemCacheStorage',
        'HTTPCACHE_IGNORE_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
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
        try:
            # Log the full HTML response for debugging
            with open(f'response_{self.search_result}.html', 'w', encoding='utf-8') as f:
                f.write(response.text)

            container = response.xpath('//div[contains(text(),"Vandaag")]/../..').get()
            if container:
                container_element = html.fromstring(container)
                items = container_element.xpath('.//div[@data-test-id="search-result-item"]')  # Adjust the XPath to match the individual item elements
                self.logger.info(f'Found {len(items)} items')

                for item in items:
                    title = item.xpath('.//h2[@data-test-id="street-name-house-number"]/text()')
                    address = item.xpath('.//div[@data-test-id="postal-code-city"]/text()')
                    images_srcset = item.xpath('.//img[contains(@class,"w-full")]/@srcset')
                    link = item.xpath('.//div[@class="flex justify-between"]/a/@href')
                    price = item.xpath('.//p[@data-test-id="price-sale"]/text()')
                    time_created = datetime.now().isoformat()

                    # Extract the first URL from the srcset attribute
                    image_url = None
                    if images_srcset:
                        image_url = images_srcset[0].split(',')[0].strip().split(' ')[0]

                    if title:
                        scraped_item = {
                            'Domain': 'funda',
                            'Title': str(title[0]).strip() if title else None,
                            'Address': str(address[0]).strip() if address else None,
                            'Image': str(image_url).strip() if image_url else None,
                            'Link': str(link[0]).strip() if link else None,
                            'Price': helper.get_price(str(price[0]).replace('.', '')) if price else None,
                            'Time Created': time_created
                        }
                        self.logger.info(f"Scraped item: {scraped_item}")
                        self.new_data.append(scraped_item)
                        yield scraped_item
                    else:
                        self.logger.info("Incomplete item, skipping.")

            if self.search_result < 30:
                self.search_result += 1
                time.sleep(3)
                yield scrapy.Request(url=f'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result={self.search_result}', callback=self.parse)
            else:
                self.logger.info("No more 'Vandaag' listings found, breaking loop")
        except Exception as e:
            self.logger.error(f"Error during parsing: {e}")

    def close(self, reason):
        # Combine existing data with new data
        self.logger.info("Closing FundaSpider")
        combined_data = self.existing_data + self.new_data

        # Save the combined data
        with open('funda_data.json', 'w') as file, os.fdopen(
                os.open(file.name, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644), 'w') as secure_file:
            json.dump(combined_data, secure_file, indent=4)
        self.logger.info(f"Saved {len(combined_data)} items to funda_data.json")


def scrape_funda():
    process = CrawlerProcess()
    process.crawl(FundaSpider)
    process.start()


if __name__ == "__main__":
    scrape_funda()
