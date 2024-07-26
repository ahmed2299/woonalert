import random
import logging

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