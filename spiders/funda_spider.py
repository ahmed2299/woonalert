import os
import sys
# Add the directory containing 'helper' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
import json
import scrapy
from scrapy.crawler import CrawlerProcess
from datetime import datetime
from lxml import html
import helper
import random

class FundaSpider(scrapy.Spider):
    name = "funda"
    start_urls = [
        'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result=1',
    ]
    # custom_settings = {
    #     'ROBOTSTXT_OBEY': False,
    #     'RETRY_ENABLED': True,
    #     'RETRY_TIMES': 5,  # Number of retries
    #     'RETRY_HTTP_CODES': [200, 500, 502, 503, 504, 522, 524, 408, 429],
    #     'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    #     'REQUEST_FINGERPRINTER_IMPLEMENTATION' : "2.7",
    #     'CONCURRENT_REQUESTS': 16,
    #     'DOWNLOAD_DELAY': 1
    # }

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'USER_AGENT': random.choice([
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'
        ]),
        'DEFAULT_REQUEST_HEADERS': {
                'authority':'www.funda.nl',
                'method':'GET',
                'path':'/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result=%221%3E%22',
                'scheme':'https',
                'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-encoding':'gzip, deflate, br, zstd',
                'accept-language' : 'en-US,en;q=0.9',
                'cache-control':'max-age=0',
                'cookie':'''i18n=nl; wib=; _ga=GA1.1.1875770340.1721594171; didomi_token=eyJ1c2VyX2lkIjoiMTkwZDcwMjQtN2E2My02ZmFlLWE4NGEtOThiYWQ2Y2M1NzQ0IiwiY3JlYXRlZCI6IjIwMjQtMDctMjFUMjA6MzY6MDkuMjU0WiIsInVwZGF0ZWQiOiIyMDI0LTA3LTIxVDIwOjM2OjExLjk3NloiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzpmdW5kYWZ1bmMtQUhoRDJZcTMiLCJjOnNlZ21lbnQtR1FwV1BRaEsiLCJjOmZ1bmRhcGFydC1pQk5nWFd3WCIsImM6ZGF0YWRvZy1yZHRZN0FtRSIsImM6Z29vZ2xlYW5hLWphZ242Tm5nIiwiYzpvcHRpbWl6ZWx5LTgyMkE2cU5nIiwiYzpjbG91ZGZsYXJlLThmbmEzRUhBIiwiYzpnb29nbGV0YWctZUVZUGozMloiLCJjOmhvdGphci1UODJqdENVcSIsImM6Z2V0ZmVlZGJhLUJ0alhGTDJaIiwiYzpzYWxlc2ZvcmNlLWtLN3JwZXhZIiwiYzppbmctanFYNmQzdFkiXX0sInB1cnBvc2VzIjp7ImVuYWJsZWQiOlsicGVyc29uYWxpc2F0aW9uLWdyb3VwIiwiYWR2ZXJ0aXNlbWVudC1ncm91cCJdfSwidmVyc2lvbiI6MiwiYWMiOiJFdC1BSUJFa0VEQUtNQWx2Z0FBQS5BQUFBIn0=; euconsent-v2=CQCGxsAQCGxsAAHABBENA9EgAP7gAAAAABpYGgNBzC5dRAFCAD5wYNsAOQQVoNAABEQgAAIAAgABwAKAIAQCkEAAFADgAAACAAAAIAIBAAJAEAAAAQAAAAAAAAAAQAAAAAIIIAAAgAIBCAAIAAAAAAAAQAAAgAACAAAAkAAAAIIAQEAABAAAAMQAAwABBSslABgACClZSADAAEFKx0AGAAIKVhIAMAAQUrLQAYAAgpWA.f9wAAAAAAAAA; didomi_consent=functional-group,analytical-group,personalisation-group,advertisement-group,cookies,measure_ad_performance,market_research,improve_products,select_basic_ads,create_ads_profile,select_personalized_ads,create_content_profile,select_personalized_content,use_limited_data_to_select_content,; userConsentPersonalization=true; _hjSessionUser_2869769=eyJpZCI6ImZhN2JlNWMwLWY4MDMtNTBkNC1hNDhiLTRkZjdkYzVkOGRjNSIsImNyZWF0ZWQiOjE3MjE1OTQxNzAyNDYsImV4aXN0aW5nIjp0cnVlfQ==; ajs_anonymous_id=d21b4186-bfdf-430d-b5c7-c1bcba3868c2; _pubcid=fd300a80-6507-4b72-9543-284c795ef870; _pubcid_cst=bSxOLDcsTQ%3D%3D; _gcl_au=1.1.1611166896.1721594174; _lr_env_src_ats=false; pbjs-unifiedid=%7B%22TDID%22%3A%22f43eaf88-48c7-44f7-bf43-3c5be3fe5d60%22%2C%22TDID_LOOKUP%22%3A%22TRUE%22%2C%22TDID_CREATED_AT%22%3A%222024-06-21T20%3A36%3A13%22%7D; pbjs-unifiedid_cst=bSxOLDcsTQ%3D%3D; .ASPXANONYMOUS=w0GjkAwC4uqxzOG0j5NgEhzqf-k7z0kPpiSHvvzzSRfWpcqD1ufQx3Ez3uDp4Onwu_raAgAhNO5HLHfrkk6fCdPGokhD4eCZ8Bf1gCEpTkLMzgAu6qujFaGuoD83guhGNZnmmSeFh-mBhf84o3HvzcJV_Nc1; optimizelyEndUserId=oeu1721599823545r0.1753502113098917; fonts-loaded=true; last_search=koop=Nederland%20%20%2B%201%20filter|L3pvZWtlbi9rb29wP3NlbGVjdGVkX2FyZWE9JTVCJTIybmwlMjIlNUQmc29ydD0lMjJkYXRlX2Rvd24lMjImcHVibGljYXRpb25fZGF0ZT0lMjIxJTIy; sessionstarted=true; panoramaId_expiry=1722074126404; _hjSession_2869769=eyJpZCI6IjZjZTRjOTU0LTY4MTQtNDVkOS1hZDY2LWI4NWI2Y2U3NzljNiIsImMiOjE3MjIwMDAzMTIwNTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; ak_bmsc=8FF0B7085D0820D88F021DC3307342BB~000000000000000000000000000000~YAAQD1CMT9U6k+uQAQAAaH837xg0Wt9vKEoKg6z6lrIXckPc/GVlxy4M8tUa90ssUlolAdDaI0imzKdfXLs4+S+WSVCqt1g5IhN4Cqh37Tm+5+yFjnm3kmhJtaW4YKEnHeIhy2wxn5EWK/M1hsyNTnZ9dRw5tr0j+yPqz4imIJfAxm42gQH9PSFdJebxCvvqgGMUBLphgPisVhre50E2+euYkiXZqZeJz4yjF8HRvMpQM6K7K4iZsSRjK3fKchHd2Od9Iukam7Ig871hBafErLdL5vB5/s52mUVHZKAPce8gXcgnKSyYExS+ZM1KFqCuYFEc0dqp1HoMN1UwlVTINqDngW6joL9/TfGi+oxsk+Sjwxz7W7uGRPprrIAu69CWj+98f8HfXIdUPZzKbFdtYGhbD6h6EGUQ7SwFXKyYXJu8EhsX4W/nTMaMu0r6h24DgG1swfcSk+9bxQ==; mp_d7f79c10b89f9fa3026f2fb08d3cf36d_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A190d70247a31368-0d3d1f496a3b71-26001f51-144000-190d70247a31368%22%2C%22%24device_id%22%3A%20%22190d70247a31368-0d3d1f496a3b71-26001f51-144000-190d70247a31368%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.upwork.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.upwork.com%22%7D; _ga_WLRNSHBY8J=GS1.1.1722000315.15.0.1722000315.0.0.0; _ga_DK6XS655BS=GS1.1.1722000315.15.0.1722000315.0.0.0; __gads=ID=3eb27eb5000d7192:T=1721594168:RT=1722000312:S=ALNI_MZntiSFvkd3ttifOw-LQAsw6iTvMw; __gpi=UID=00000e9b4a06d00a:T=1721594168:RT=1722000312:S=ALNI_MbfzezLTwZa7Kl2499VKonuGF5q3g; __eoi=ID=e02d3426a3ac0fb6:T=1721594168:RT=1722000312:S=AA-AfjbE78AnbdeT8doBTMsSqOEa; _lr_retry_request=true; cto_bundle=e4ubaV9LaldhRFAlMkJRZTJ5dHVHRVVnMVpOWjUxOG90c2JyTVdMVmVzekR0OG1ManZLMDN4UVY3MWptbW03eWFRS2hzbFlpVFhDTFJHZkxJZUlVaGM0cWZiY0JwQ2pLJTJGN1V0RFVrQWlHdlJ5ZkY4QThsQmprdnFPMHI0RWNUNWclMkIxU244U1Nsc3Q1cWNQV2k4RXFvOTRmVjlnV3clM0QlM0Q; cto_bidid=qL7kil9rVHIlMkZhMjJXJTJGRlB4ZWFZdlAlMkJubURmV005JTJGMTh4U2l2RmF6Q3ZXN0NFUU9GR0pGWEZsNXBmMm51UkwwaCUyQkVLcnFMT1pPRlo1dTJtczIyZGNSMmt4YWJIVjl0UEluMlQwcHgyRzhrZHRqZkklM0Q; _dd_s=logs=1&id=21b04455-b7fb-4608-99d9-d505a875a2b0&created=1722000311738&expire=1722001211738''',
                'priority':'u=0, i',
                'sec-ch-ua':'''"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"''',
                'sec-ch-ua-mobile':'?0',
                'sec-ch-ua-platform':'"Windows"',
                'sec-fetch-dest':'document',
                'sec-fetch-mode':'navigate',
                'sec-fetch-site':'none',
                'sec-fetch-user':'?1',
                'upgrade-insecure-requests':'1',
                'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
        },
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
        },
        'HTTPPROXY_ENABLED': True,
        'HTTP_PROXY': '104.167.26.204:3128'
    }
    def __init__(self):
        super().__init__()
        self.search_result = 1
        self.existing_data = []
        self.new_data = []

    def start_requests(self):
        # Load existing data
        if os.path.exists('funda_data.json'):
            with open('funda_data.json', 'r') as file:
                try:
                    self.existing_data = json.load(file)
                except json.JSONDecodeError:
                    self.existing_data = []

        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # self.log(f"Parsing page {self.search_result}", level=scrapy.log.INFO)

        items = response.xpath(
            '//div[@class="pt-4"]//div[contains(text(),"Vandaag")]/../../div[contains(@class, "border-neutral-20") and contains(@class, "mb-4") and contains(@class, "border-b") and contains(@class, "pb-4")]').getall()
        # self.log(f"Found {len(items)} items", level=scrapy.log.INFO)

        if not items:
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

        # Follow the next page link
        self.search_result += 1
        next_page = f'https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&publication_date=%221%22&search_result={self.search_result}'
        yield scrapy.Request(next_page, callback=self.parse)

    def close(self, reason):
        # Combine existing data with new data
        combined_data = self.existing_data + self.new_data

        # Save the combined data
        with open('funda_data.json', 'w') as file:
            json.dump(combined_data, file, indent=4)


def scrape_funda():
    process = CrawlerProcess()
    process.crawl(FundaSpider)
    process.start()


if __name__ == "__main__":
    scrape_funda()
