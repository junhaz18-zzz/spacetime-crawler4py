import os
import shelve

from threading import RLock
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = []
        self.lock = RLock()

        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        # 初始化 frontier（只在主线程做一次）
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.to_be_downloaded:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _open_save(self):
        """
        每次访问 shelve 时单独打开，避免跨线程共享 SQLite connection
        """
        return shelve.open(self.config.save_file)

    def _parse_save_file(self):
        total_count = 0
        tbd_count = 0

        with self._open_save() as save:
            total_count = len(save)
            for url, completed in save.values():
                if not completed and is_valid(url):
                    self.to_be_downloaded.append(url)
                    tbd_count += 1

        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        with self.lock:
            try:
                return self.to_be_downloaded.pop(0)
            except IndexError:
                return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)

        with self.lock:
            with self._open_save() as save:
                if urlhash not in save:
                    save[urlhash] = (url, False)
                    save.sync()
                    self.to_be_downloaded.append(url)

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)

        with self.lock:
            with self._open_save() as save:
                if urlhash not in save:
                    self.logger.error(
                        f"Completed url {url}, but have not seen it before.")
                else:
                    save[urlhash] = (url, True)
                    save.sync()
