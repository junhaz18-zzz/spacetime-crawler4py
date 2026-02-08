from threading import Thread
from inspect import getsource

from utils.download import download
from utils import get_logger
import scraper
import analytics as analytics_mod


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        super().__init__(daemon=True)
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        assert {getsource(scraper).find(req) for req in {"import requests", "from requests import"}} == {-1}
        assert {getsource(scraper).find(req) for req in {"import urllib.request", "from urllib.request import"}} == {-1}

    def run(self):
        while True:
            url = self.frontier.get_tbd_url()
            if url is None:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            resp = download(url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {url}, status <{resp.status}>, using cache {self.config.cache_server}."
            )

            should_scrape = True
            try:
                if resp.status == 200 and resp.raw_response and resp.raw_response.content:
                    should_scrape = analytics_mod.process_page(url, resp.raw_response.content)
            except Exception as e:
                self.logger.error(f"Analytics error on {url}: {e}")

            if should_scrape:
                scraped = scraper.scraper(url, resp)
                self.logger.info(f"Scraped {len(scraped)} urls from {url}")
                for new_url in scraped:
                    self.frontier.add_url(new_url)

            self.frontier.mark_url_complete(url)

