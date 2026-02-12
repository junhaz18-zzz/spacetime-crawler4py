import re
from urllib.parse import urljoin, urldefrag, urlparse

from bs4 import BeautifulSoup
from validator import is_valid

# 防止爬去local host / invalid ip adress
_BAD_HOST_HINTS = (
    "your_ip", "localhost", "127.0.0.1", "::1"
)

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Status Check. If status code is not 200, return [].
    if resp.status != 200:
        return []

    if not resp.raw_response or not resp.raw_response.content:
        return []

    # Content-Type Check 
    # Prevents parsing binary files. 確定只parse html. 檢查header,不解析pdf和任何圖片
    headers = getattr(resp.raw_response, 'headers', {})
    content_type = headers.get('Content-Type', '').lower()
    if 'text' not in content_type and 'html' not in content_type:
        return []

    content = resp.raw_response.content

    # Size Check。文件大小限制為10MB
    if len(content) > 10 * 1024 * 1024: # 10MB limit
        return []

    base_url = resp.url if resp.url else url

    # Decode content safely. Use replace to handle bad encoding bytes. 
    if isinstance(content, (bytes, bytearray)):
        html = content.decode("utf-8", errors="replace")
    else:
        html = str(content)

    soup = BeautifulSoup(html, "html.parser")

    # Low-Information/404 Detection 過濾掉apache/nginx的index of目錄
    
    # Check Title for errors or directory listings
    if soup.title and soup.title.string:
        title = soup.title.string.lower()
        if "index of" in title:  # Directory listing
            return []
        if "404" in title or "page not found" in title:
            return []
        if "500" in title or "internal server error" in title:
            return []
            
    # Word Count Check
    # "get_text" removes tags，split by whitespace to count the actual words.
    text = soup.get_text(separator=" ", strip=True)
    word_count = len(text.split())
    
    # 如果少於50個詞，直接丟棄，low info.
    if word_count < 50: 
        return []

    # Body Text Check for 404, 過濾不存在但反饋200的page
    tl = text.lower()
    if "page not found" in tl or "no results found" in tl:
        return []

    # Extract Links
    extracted = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href or not href.strip():
            continue

        low = href.lower()
        # Filter non-web protocols
        if low.startswith(("mailto:", "javascript:", "tel:", "ftp:", "file:")):
            continue
		# prevent crawling weird placeholders
        if "your_ip" in low:
            continue
		# convert relative url to absolute url
        try:
            abs_url = urljoin(base_url, href)
        except ValueError:
            continue
		# 去掉#fragment部分
        abs_url, _frag = urldefrag(abs_url)
        abs_url = abs_url.strip()
        if not abs_url:
            continue

        # Hostname safety check，防止解析出來的host包含非法字符
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