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

        # Thread-safe frontier queue
        self.to_be_downloaded = Queue()
        self.lock = RLock()

        # Per-domain politeness tracking
        self._domain_next_allowed = {}

        # Handle restart / resume
        if restart and os.path.exists(self.config.save_file):
            self.logger.info(f"Restart enabled. Deleting {self.config.save_file}.")
            os.remove(self.config.save_file)

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._load_from_save()
            if self.to_be_downloaded.empty():
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _open_save(self):
        # Open shelve on demand (thread-safe usage)
        return shelve.open(self.config.save_file)

    def _load_from_save(self):
        with self._open_save() as save:
            for url, completed in save.values():
                if not completed and is_valid(url):
                    self.to_be_downloaded.put(url)

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)

        with self.lock:
            with self._open_save() as save:
                if urlhash not in save:
                    save[urlhash] = (url, False)
                    save.sync()
                    self.to_be_downloaded.put(url)

    def get_tbd_url(self):
        try:
            url = self.to_be_downloaded.get(timeout=1)
        except Empty:
            return None

        self._wait_for_politeness(url)
        return url

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.lock:
            with self._open_save() as save:
                save[urlhash] = (url, True)
                save.sync()

    def _wait_for_politeness(self, url):
        try:
            host = urlparse(url).netloc.lower()
        except Exception:
            host = ""

        delay = getattr(self.config, "time_delay", 0.5)
        now = time.monotonic()

        with self.lock:
            next_allowed = self._domain_next_allowed.get(host, 0.0)
            wait_time = max(0.0, next_allowed - now)
            self._domain_next_allowed[host] = max(now, next_allowed) + delay

        if wait_time > 0:
            time.sleep(wait_time)
