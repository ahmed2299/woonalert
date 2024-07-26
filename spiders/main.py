import logging
from scrapy.crawler import CrawlerProcess
from pararius_spider import ParariusSpider
from funda_spider import FundaSpider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape():
    process = CrawlerProcess()
    process.crawl(ParariusSpider)
    process.crawl(FundaSpider)
    process.start()

if __name__ == "__main__":
    scrape()
