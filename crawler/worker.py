from threading import Thread
from inspect import getsource

from utils.download import download
from utils import get_logger
import scraper

import analytics as analytics_mod  # module file style


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, \
            "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, \
            "Do not use urllib.request in scraper.py"

        super().__init__(daemon=True)

    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            # per-domain politeness + log domain + sleep_ms
            self.frontier.wait_for_politeness(tbd_url, logger=self.logger)

            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}."
            )

            should_scrape = True

            # analytics (best-effort)
            try:
                if resp.status == 200 and resp.raw_response and resp.raw_response.content:
                    # UPDATED: process_page returns False if it's a near-duplicate
                    should_scrape = analytics_mod.process_page(tbd_url, resp.raw_response.content)
            except Exception as e:
                self.logger.error(f"Analytics error on {tbd_url}: {e}")

            # NEW: Only scrape if it is NOT a duplicate
            if should_scrape:
                scraped_urls = scraper.scraper(tbd_url, resp)
                self.logger.info(f"Scraped {len(scraped_urls)} urls from {tbd_url}")

                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            else:
                self.logger.info(f"Skipped scraping {tbd_url} (Near-Duplicate detected)")

            self.frontier.mark_url_complete(tbd_url)
