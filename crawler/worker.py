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

        # Enforce assignment restriction
        assert {getsource(scraper).find(req) for req in {"import requests", "from requests import"}} == {-1}
        assert {getsource(scraper).find(req) for req in {"import urllib.request", "from urllib.request import"}} == {-1}

    def run(self):
        while True:
            url = self.frontier.get_tbd_url()
            if url is None:
                break

            resp = download(url, self.config, self.logger)

            try:
                if resp.status == 200 and resp.raw_response and resp.raw_response.content:
                    analytics_mod.process_page(url, resp.raw_response.content)
            except Exception as e:
                self.logger.error(f"Analytics error on {url}: {e}")

            for new_url in scraper.scraper(url, resp):
                self.frontier.add_url(new_url)

            self.frontier.mark_url_complete(url)
