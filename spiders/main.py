import logging
from scrapy.crawler import CrawlerProcess
from spiders.pararius_spider import ParariusSpider
from spiders.funda_spider import FundaSpider

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def scrape():
    process = CrawlerProcess()
    process.crawl(ParariusSpider)
    process.crawl(FundaSpider)
    process.start()

if __name__ == "__main__":
    scrape()
