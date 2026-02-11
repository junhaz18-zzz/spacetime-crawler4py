from threading import Thread
from inspect import getsource

from utils.download import download
from utils import get_logger
import scraper
import analytics as analytics_mod


class Worker(Thread):
    # Worker thread that downloads pages and extracts new URLs
    def __init__(self, worker_id, config, frontier):
        # start a worker thread(设成True，这样主线程结束时不会卡住)
        super().__init__(daemon=True)
        
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        # make sure scraper does NOT directly import requests or urllib
        assert {getsource(scraper).find(req) for req in {"import requests", "from requests import"}} == {-1}
        assert {getsource(scraper).find(req) for req in {"import urllib.request", "from urllib.request import"}} == {-1}

    # Main loop of the worker: get URL, download, analyze, scrape, repeat
    def run(self):
        # 每个worker一直跑，直到frontier空了
        while True:
            url = self.frontier.get_tbd_url()
            if url is None:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            # Download the page
            resp = download(url, self.config, self.logger)
            
            self.logger.info(
                f"Downloaded {url}, status <{resp.status}>, using cache {self.config.cache_server}."
            )

            # 默认是要scrape的
            should_scrape = True
            try:
                # 只有status200 + 有内容，才送去analytics
                if resp.status == 200 and resp.raw_response and resp.raw_response.content:
                    # analytics决定是否继续scrape
                    should_scrape = analytics_mod.process_page(url, resp.raw_response.content)
            except Exception as e:
                # if analytics fails, just log it, don’t crash crawler
                self.logger.error(f"Analytics error on {url}: {e}")

            if should_scrape:
                scraped = scraper.scraper(url, resp)
                self.logger.info(f"Scraped {len(scraped)} urls from {url}")
                for new_url in scraped:
                    self.frontier.add_url(new_url)

            # mark this URL as finished (avoid re-crawling)
            self.frontier.mark_url_complete(url)

