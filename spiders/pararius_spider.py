import json
import os
import sys
# Add the directory containing 'helper' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Now you can import 'helper' from the correct path
import helper
import scrapy
from scrapy.crawler import CrawlerProcess
from datetime import datetime
from lxml import html
import subprocess

class ParariusSpider(scrapy.Spider):
    name = "pararius"
    start_urls = [
        'https://www.pararius.nl/koopwoningen/nederland/sinds-1'
    ]
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,  # Number of retries
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    def __init__(self):
        super().__init__()
        self.existing_data = []
        self.new_data = []

    def start_requests(self):
        # Load existing data
        if os.path.exists('pararius_data.json'):
            with open('pararius_data.json', 'r') as file:
                try:
                    self.existing_data = json.load(file)
                except json.JSONDecodeError:
                    self.existing_data = []

        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Pass the response to populate_item
        yield from self.populate_item(response)

        # Get the next page URL and yield a new request
        next_page = response.xpath('//a[@class="pagination__link pagination__link--next"]/@href').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def populate_item(self, response):
        items = response.xpath(
            '//div[@class="page__row page__row--search-list"]//section[@class="listing-search-item listing-search-item--list listing-search-item--for-sale"]').getall()

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
        # Combine existing data with new data
        combined_data = self.existing_data + self.new_data

        # Save the combined data
        with open('pararius_data.json', 'w') as file:
            json.dump(combined_data, file, indent=4)

def scrape_pararius():
    process = CrawlerProcess()
    process.crawl(ParariusSpider)
    process.start()

if __name__ == "__main__":
    scrape_pararius()

# def scrape_funda():
#     subprocess.run(["scrapy", "crawl", "pararius"])
#
# if __name__ == "__main__":
#     scrape_funda()

