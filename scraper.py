import re
from urllib.parse import urljoin, urldefrag, urlparse

from bs4 import BeautifulSoup
from validator import is_valid

_BAD_HOST_HINTS = (
    "your_ip", "localhost", "127.0.0.1", "::1"
)

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # 1. Status Check
    if resp.status != 200:
        return []

    if not resp.raw_response or not resp.raw_response.content:
        return []

    # 2. Content-Type Check 
    # Prevents parsing binary files (PDFs, Images) that miss the extension block in validator
    headers = getattr(resp.raw_response, 'headers', {})
    content_type = headers.get('Content-Type', '').lower()
    if 'text' not in content_type and 'html' not in content_type:
        return []

    content = resp.raw_response.content

    # 3. Size Check 
    if len(content) > 10 * 1024 * 1024: # 10MB limit
        return []

    base_url = resp.url if resp.url else url

    # Decode content safely
    if isinstance(content, (bytes, bytearray)):
        html = content.decode("utf-8", errors="replace")
    else:
        html = str(content)

    soup = BeautifulSoup(html, "html.parser")

    # 4. Low-Information / Soft-404 Detection
    
    # A. Check Title for errors or directory listings
    if soup.title and soup.title.string:
        title = soup.title.string.lower()
        if "index of" in title:  # Directory listing
            return []
        if "404" in title or "page not found" in title:
            return []
        if "500" in title or "internal server error" in title:
            return []
            
    # B. Word Count Check
    # "get_text" removes tags. We split by whitespace.
    text = soup.get_text(separator=" ", strip=True)
    word_count = len(text.split())
    
    # 50 is a safer threshold for valuable content.
    if word_count < 50: 
        return []

    # C. Body Text Check for soft-404s
    tl = text.lower()
    if "page not found" in tl or "no results found" in tl:
        return []

    # 5. Extract Links
    extracted = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href or not href.strip():
            continue

        low = href.lower()
        # Filter non-web protocols
        if low.startswith(("mailto:", "javascript:", "tel:", "ftp:", "file:")):
            continue

        if "your_ip" in low:
            continue

        try:
            abs_url = urljoin(base_url, href)
        except ValueError:
            continue

        abs_url, _frag = urldefrag(abs_url)
        abs_url = abs_url.strip()
        if not abs_url:
            continue

        # Hostname safety check
        try:
            parsed = urlparse(abs_url)
            host = parsed.netloc.lower()
            if not host:
                continue
            if any(h in host for h in _BAD_HOST_HINTS):
                continue
            if "[" in host or "]" in host:
                continue
        except Exception:
            continue

        extracted.add(abs_url)

    return list(extracted)