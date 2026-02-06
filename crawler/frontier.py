import os
import shelve
import time
from threading import RLock
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = []
        self.lock = RLock()

        # per-domain politeness (monotonic timestamp)
        self._domain_next_allowed = {}

        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, starting from seed."
            )
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.to_be_downloaded:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _open_save(self):
        # 关键：按需打开，避免 SQLite connection 跨线程共享
        return shelve.open(self.config.save_file)

    def _parse_save_file(self):
        total_count = 0
        tbd_count = 0

        with self._open_save() as save:
            total_count = len(save)
            for url, completed in save.values():
                if (not completed) and is_valid(url):
                    self.to_be_downloaded.append(url)
                    tbd_count += 1

        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} total urls discovered."
        )

    def get_tbd_url(self):
        with self.lock:
            if not self.to_be_downloaded:
                return None
            return self.to_be_downloaded.pop(0)

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)

        with self.lock:
            with self._open_save() as save:
                if urlhash not in save:
                    save[urlhash] = (url, False)
                    save.sync()
                    self.to_be_downloaded.append(url)

    def get_next_url(self):
        while True:
            with self.lock:
                if not self.to_be_downloaded:
                    return None
                url = self.to_be_downloaded.pop(0)
    
            self.wait_for_politeness(url, logger=self.logger)
            return url

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)

        with self.lock:
            with self._open_save() as save:
                if urlhash not in save:
                    self.logger.error(f"Completed url {url}, but have not seen it before.")
                save[urlhash] = (url, True)
                save.sync()

    def wait_for_politeness(self, url, logger=None):
        """
        per-domain politeness:
        - 同一 domain 的两次请求至少间隔 config.time_delay（作业要求多线程也必须满足 500ms）
        - 打印 domain + sleep_ms
        """
        if logger is None:
            logger = self.logger

        try:
            host = urlparse(url).netloc.lower().split(":")[0]
        except Exception:
            host = ""

        delay = getattr(self.config, "time_delay", 0.5)
        if not delay or delay <= 0:
            delay = 0.5

        now = time.monotonic()

        with self.lock:
            next_allowed = self._domain_next_allowed.get(host, 0.0)
            wait = 0.0
            if next_allowed > now:
                wait = next_allowed - now

            # 预定下一次可访问时间（多线程关键）
            base = next_allowed if next_allowed > now else now
            self._domain_next_allowed[host] = base + delay

        wait_ms = int(wait * 1000)
        logger.info(f"Politeness: domain={host}, sleep_ms={wait_ms}")

        if wait > 0:
            time.sleep(wait)

