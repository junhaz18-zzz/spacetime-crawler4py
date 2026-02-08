import re
from urllib.parse import urljoin, urldefrag, urlparse

from bs4 import BeautifulSoup
from validator import is_valid


_BAD_HOST_HINTS = (
    "your_ip", "localhost", "127.0.0.1", "::1"
)


def scraper(url, resp):
    raw_links = extract_next_links(url, resp)
    return [link for link in raw_links if is_valid(link)]


def extract_next_links(url, resp):
    if resp.status != 200:
        return []

    if not resp.raw_response or not resp.raw_response.content:
        return []

    content = resp.raw_response.content

    # Avoid very large files (>10MB)
    if len(content) > 10 * 1024 * 1024:
        return []

    base_url = resp.url if resp.url else url

    # Decode ourselves to avoid BS/lxml "REPLACEMENT CHARACTER" spam
    if isinstance(content, (bytes, bytearray)):
        html = content.decode("utf-8", errors="replace")
    else:
        html = str(content)

    # parser choice: html.parser is stable; lxml may not be installed everywhere
    soup = BeautifulSoup(html, "html.parser")

    # Low-info / soft-404 filter (keep, but make it less aggressive if you want coverage)
    text = soup.get_text(separator=" ", strip=True)
    word_count = len(text.split())
    if word_count < 20:
        return []

    tl = text.lower()
    if "page not found" in tl or "no results found" in tl:
        return []

    extracted = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href:
            continue
        href = href.strip()
        if not href:
            continue

        low = href.lower()
        if low.startswith(("mailto:", "javascript:", "tel:")):
            continue

        # quick drop placeholder IPv6-bracket style or bad hints
        if "your_ip" in low:
            continue

        try:
            abs_url = urljoin(base_url, href)
        except ValueError:
            # e.g. http://[YOUR_IP]/... triggers strict IPv6 validation
            continue

        abs_url, _frag = urldefrag(abs_url)
        abs_url = abs_url.strip()
        if not abs_url:
            continue

        # another quick safety: drop bracketed netloc placeholders
        try:
            host = urlparse(abs_url).netloc.lower()
            if any(h in host for h in _BAD_HOST_HINTS):
                continue
            if "[" in host or "]" in host:
                continue
        except Exception:
            continue

        extracted.add(abs_url)

    return list(extracted)
