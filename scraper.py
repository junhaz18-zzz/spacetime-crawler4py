import re
from urllib.parse import urljoin, urldefrag

from bs4 import BeautifulSoup

# IMPORTANT:
# 按作业要求，你应该在 scraper.py 里实现/修改 is_valid
# 下面先保留导入，若你已有 validator.py 并且老师允许，也可继续用；
# 但建议你把 validator.is_valid 的逻辑挪到这里，避免不符合 spec。
from validator import is_valid


def scraper(url, resp):
    raw_links = extract_next_links(url, resp)
    valid_links = [link for link in raw_links if is_valid(link)]
    return valid_links



def extract_next_links(url, resp):
    if resp.status != 200:
        return []

    if not resp.raw_response or not resp.raw_response.content:
        return []

    # Avoid very large files (>10MB)
    if len(resp.raw_response.content) > 10 * 1024 * 1024:
        return []

    base_url = resp.url if resp.url else url

    try:
        # 如果 lxml 没装，BeautifulSoup 会退回内置 html.parser
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")

    extracted = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href:
            continue

        # Drop non-web links early
        low = href.lower()
        if low.startswith("mailto:") or low.startswith("javascript:") or low.startswith("tel:"):
            continue

        abs_url = urljoin(base_url, href)

        # Defragment (required by spec)
        abs_url, _frag = urldefrag(abs_url)

        # Optional normalization: strip trailing spaces
        abs_url = abs_url.strip()

        extracted.add(abs_url)

    return list(extracted)
