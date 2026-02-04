import re
from collections import Counter
from threading import RLock
from urllib.parse import urlparse, urldefrag

from bs4 import BeautifulSoup

# 你原来的 STOP_WORDS（这里省略：请保留你文件里的那一大段 STOP_WORDS）
STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't",
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's",
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves"
}

_lock = RLock()

# 增量统计的全局状态
_unique_urls = set()              # defragment 后的 unique pages
_word_counter = Counter()         # 全局词频
_subdomain_counter = Counter()    # uci.edu 下每个 hostname 的 unique pages 数

_longest_url = None
_longest_word_count = 0


def _html_to_text(html_content: bytes) -> str:
    """HTML -> 纯文本（markup 不算 words）。"""
    if not html_content:
        return ""

    if isinstance(html_content, bytes):
        html_str = html_content.decode("utf-8", errors="ignore")
    else:
        html_str = str(html_content)

    soup = BeautifulSoup(html_str, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def process_page(url: str, html_content: bytes) -> None:
    """
    Worker 每抓到一个 200 页面就调用一次。
    负责更新：
      - unique pages（defrag url）
      - longest page by word count
      - top 50 words（stopwords ignored）
      - subdomain stats under uci.edu
    """
    global _longest_url, _longest_word_count

    if not url:
        return

    # defragment
    url, _ = urldefrag(url)

    text = _html_to_text(html_content)

    # tokenize
    tokens = re.findall(r"[a-zA-Z0-9]+", text.lower())
    tokens = [t for t in tokens if t not in STOP_WORDS and len(t) > 1]
    wc = len(tokens)

    parsed = urlparse(url)
    host = (parsed.netloc or "").lower().split(":")[0]

    with _lock:
        # unique pages
        if url not in _unique_urls:
            _unique_urls.add(url)

            # subdomain count (严格判断，别用 "uci.edu" in host 那种宽匹配)
            if host == "uci.edu" or host.endswith(".uci.edu"):
                _subdomain_counter[host] += 1

        # longest page
        if wc > _longest_word_count:
            _longest_word_count = wc
            _longest_url = url

        # global word freq
        _word_counter.update(tokens)


def finalize_report():
    """返回 4 个指标的快照，用于写 report。"""
    with _lock:
        unique_pages = len(_unique_urls)
        longest_url = _longest_url
        longest_wc = _longest_word_count
        top_50 = _word_counter.most_common(50)
        subdomains = dict(_subdomain_counter)

    return unique_pages, longest_url, longest_wc, top_50, subdomains


def write_report(filepath: str = "report.txt") -> None:
    unique_pages, longest_url, longest_wc, top_50, subdomains = finalize_report()

    lines = []
    lines.append(f"1. Unique pages: {unique_pages}")
    lines.append(f"2. Longest page: {longest_url} ({longest_wc} words)")
    lines.append("3. Top 50 words:")
    for w, c in top_50:
        lines.append(f"   {w}: {c}")
    lines.append("4. Subdomains in uci.edu (alphabetical):")
    for sd in sorted(subdomains.keys()):
        lines.append(f"   {sd}, {subdomains[sd]}")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def print_report() -> None:
    unique_pages, longest_url, longest_wc, top_50, subdomains = finalize_report()

    print("-" * 40)
    print(f"Unique pages: {unique_pages}")
    print(f"Longest Page: {longest_url} ({longest_wc} words)")
    print("-" * 40)
    print("Top 50 Most Common Words:")
    for w, c in top_50:
        print(f"{w}: {c}")
    print("-" * 40)
    print("Subdomains in uci.edu (ordered alphabetically):")
    for sd in sorted(subdomains.keys()):
        print(f"{sd}, {subdomains[sd]}")
    print("-" * 40)
