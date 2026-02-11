import os
import shelve
import time
from threading import RLock
from urllib.parse import urlparse
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        # 这个queue是存还没download的url
        self.to_be_downloaded = Queue()
        
        # multithread
        self.lock = RLock()
       
        #for politeness
        self._domain_next_allowed = {}

        # if restart is True, wipe old shelve file
        if restart and os.path.exists(self.config.save_file):
            self.logger.info(f"Restart enabled. Deleting {self.config.save_file}.")
            os.remove(self.config.save_file)

        # fresh start
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._load_from_save()
            if self.to_be_downloaded.empty():
                for url in self.config.seed_urls:
                    self.add_url(url)

    # open shelve file
    def _open_save(self):
        return shelve.open(self.config.save_file)
    
    # reload unfinished urls into queue
    def _load_from_save(self):
        with self._open_save() as save:
            for url, completed in save.values():
                if not completed and is_valid(url):
                    self.to_be_downloaded.put(url)

    def add_url(self, url):
        url = normalize(url)    # avoid duplicates like trailing slash issues
        urlhash = get_urlhash(url)

        with self.lock:
            with self._open_save() as save:
                # 如果之前没见过这个url才加
                if urlhash not in save:
                    save[urlhash] = (url, False)    # 先标记为未完成
                    save.sync()
                    self.to_be_downloaded.put(url)

    # try to get a url from queue
    def get_tbd_url(self):
        try:
            url = self.to_be_downloaded.get(timeout=1)
        except Empty:
            return None

        self._wait_for_politeness(url)
        return url

    # after worker finishes downloading
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.lock:
            with self._open_save() as save:
                save[urlhash] = (url, True)
                save.sync()

    def _wait_for_politeness(self, url):
        # extract domain
        try:
            host = urlparse(url).netloc.lower().split(":")[0]
        except Exception:
            host = ""

        if host.startswith("www."):
            host = host[4:]

        # 默认delay是0.5秒（如果config里没设）
        delay = getattr(self.config, "time_delay", 0.5)
        if not delay or delay <= 0:
            delay = 0.5

        now = time.monotonic()

        with self.lock:
            # check when this domain is next allowed
            next_allowed = self._domain_next_allowed.get(host, 0.0)

            # calculate how long we need to wait
            wait_time = max(0.0, next_allowed - now)

            # update next allowed time
            self._domain_next_allowed[host] = max(now, next_allowed) + delay

        wait_ms = int(wait_time * 1000)
        self.logger.info(f"Politeness: domain={host}, sleep_ms={wait_ms}")

        # 如果需要等，就sleep
        if wait_time > 0:
            time.sleep(wait_time)

