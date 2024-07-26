import random
import time
from scrapy import signals
from scrapy.http import HtmlResponse
from requests_html import HTMLSession


class RetryMiddleware:

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        session = HTMLSession()
        response_html = session.get(request.url, timeout=30)
        response_html.raise_for_status()  # Ensure we catch any HTTP errors
        time.sleep(random.uniform(0.3, 0.6))
        page_content = response_html.text
        response = HtmlResponse(url=request.url, body=page_content, encoding='utf-8')

        items = response.xpath(
            '//div[@class="pt-4"]//div[contains(text(),"Vandaag")]/../../div[contains(@class, "border-neutral-20") and contains(@class, "mb-4") and contains(@class, "border-b") and contains(@class, "pb-4")]').getall()

        if not items:
            spider.logger.info(f"No items found, retrying {request.url}")
            retries = request.meta.get('retry_times', 0)
            if retries < 20:
                retries += 1
                new_request = request.copy()
                new_request.meta['retry_times'] = retries
                new_request.dont_filter = True
                return new_request
            else:
                spider.logger.error(f"Failed to retrieve data after {retries} retries: {request.url}")
        else:
            spider.logger.info(f"Successfully retrieved data from {request.url}")
            return response

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
