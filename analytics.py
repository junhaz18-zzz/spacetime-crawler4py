import re
from collections import Counter
from threading import RLock
from urllib.parse import urlparse, urldefrag

from bs4 import BeautifulSoup

# STOP WORD list
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

# thread lock to prevent race conditions, 防止多線程導致混亂
_lock = RLock()

# global analytics
_unique_urls = set() # set of all uniqer urls
_word_counter = Counter() # count word frequency
_subdomain_counter = Counter() # count pages per sub domian
_longest_url = None # url with most word
_longest_word_count = 0

# duplication globals
_exact_fingerprints = set()     # exact duplicate detection
_simhash_fps = []               # store simhash values for near duplicate check
_near_duplicate_count = 0		# counter for how many duplicate found

# 將64位simhash分為4段，每段16位，如果兩個hash在任意一段完全相同->candidate for duplication
_bucket_index = {}

# tuning knobs
SIMHASH_BITS = 64
# near-dup threshold 
NEAR_DUP_THRESHOLD = 4

# split 64-bit into 4 bands of 16-bit, 加速查找
BAND_BITS = 16
BAND_COUNT = SIMHASH_BITS // BAND_BITS
BAND_MASK = (1 << BAND_BITS) - 1

def _fnv1a_64_bytes(data: bytes) -> int:
    """FNV-1a 64-bit， hash algorithm"""
    h = 1469598103934665603  # offset basis 初始偏移
    prime = 1099511628211
    for b in data:
        h ^= b
        h = (h * prime) & ((1 << 64) - 1) # multiply and mask to 64 bits
    return h

def _fnv1a_64_str(s: str) -> int:
    """wrapper to hash a string directly"""
    return _fnv1a_64_bytes(s.encode("utf-8", errors="ignore"))

def _hamming_distance_64(a: int, b: int) -> int:
    x = a ^ b
    # caluculate number of differing bits between two integers. 
    return x.bit_count()

# html -> text
def _html_to_text(html_content: bytes) -> str:
    """extract visible text from html bytes"""
    if not html_content:
        return ""
    if isinstance(html_content, bytes):
        html_str = html_content.decode("utf-8", errors="ignore") # ignore bad chars
    else:
        html_str = str(html_content)

    soup = BeautifulSoup(html_str, "html.parser")
    return soup.get_text(separator=" ", strip=True)

# tokenization + weights (no Counter for duplication)

def _tokenize(text: str):
    tokens = re.findall(r"[a-zA-Z0-9]+", text.lower()) #只保留字母數字，轉小寫
    out = []
    for t in tokens:
        if len(t) > 1 and t not in STOP_WORDS:
            out.append(t)
    return out

def _term_frequencies(tokens):
    tf = {}
    for t in tokens:
        tf[t] = tf.get(t, 0) + 1
    return tf

# exact fingerprint 
def _normalize_for_fingerprint(tokens):
    """
    Join tokens with spaces (already lowercased and stopword-filtered).
    """
    return " ".join(tokens[:5000])

def _compute_exact_fingerprint(tokens) -> int:
    norm = _normalize_for_fingerprint(tokens)
    return _fnv1a_64_str(norm)

# simhash implementation

def _compute_simhash(tf: dict, bits=SIMHASH_BITS) -> int:
    if not tf:
        return 0

    v = [0] * bits

    for term, w in tf.items():
        h = _fnv1a_64_str(term)  # 64-bit term hash
        for i in range(bits):
            if h & (1 << i):
                v[i] += w
            else:
                v[i] -= w

    fp = 0
    for i in range(bits):
        if v[i] > 0:
            fp |= (1 << i)
    return fp


def _bands(simhash_fp: int):
    """Yield (band_id, band_value) pairs."""
    for band_id in range(BAND_COUNT):
        shift = band_id * BAND_BITS
        yield band_id, (simhash_fp >> shift) & BAND_MASK


def _is_near_duplicate(simhash_fp: int, threshold=NEAR_DUP_THRESHOLD) -> bool:
    """
    High performance near-duplicate check:
    - get candidates from bucket index (same band value)
    - compute exact hamming only on candidates
    """
    candidate_ids = set()

    for band_id, band_val in _bands(simhash_fp):
        key = (band_id, band_val)
        ids = _bucket_index.get(key)
        if ids:
            for idx in ids:
                candidate_ids.add(idx)

    # no candidates => definitely not near-dup
    if not candidate_ids:
        return False

    for idx in candidate_ids:
        if _hamming_distance_64(simhash_fp, _simhash_fps[idx]) <= threshold:
            return True

    return False


def _index_simhash(simhash_fp: int, idx: int) -> None:
    for band_id, band_val in _bands(simhash_fp):
        key = (band_id, band_val)
        if key not in _bucket_index:
            _bucket_index[key] = [idx]
        else:
            _bucket_index[key].append(idx)

# main entry: called by Worker

def process_page(url: str, html_content: bytes) -> bool:
    """
    Returns:
      True: OK to scrape links from this page
      False:do not scrape links (duplicate / near-duplicate)
    """
    global _longest_url, _longest_word_count, _near_duplicate_count

    if not url:
        return False

    url, _ = urldefrag(url)

    text = _html_to_text(html_content)
    tokens = _tokenize(text)
    wc = len(tokens)

    parsed = urlparse(url)
    host = (parsed.netloc or "").lower().split(":")[0]

    # duplication signals
    tf = _term_frequencies(tokens)
    exact_fp = _compute_exact_fingerprint(tokens)
    simhash_fp = _compute_simhash(tf)

    should_scrape = True

    with _lock:
        # exact duplicate
        if exact_fp in _exact_fingerprints:
            _near_duplicate_count += 1
            should_scrape = False
        else:
            _exact_fingerprints.add(exact_fp)

            # near duplicate
            if _is_near_duplicate(simhash_fp):
                _near_duplicate_count += 1
                should_scrape = False
            else:
                idx = len(_simhash_fps)
                _simhash_fps.append(simhash_fp)
                _index_simhash(simhash_fp, idx)

        # analytics bookkeeping
        if url not in _unique_urls:
            _unique_urls.add(url)
            if host == "uci.edu" or host.endswith(".uci.edu"):
                _subdomain_counter[host] += 1

        if wc > _longest_word_count:
            _longest_word_count = wc
            _longest_url = url

        _word_counter.update(tokens)

    return should_scrape


def finalize_report():
    with _lock:
        unique_pages = len(_unique_urls)
        longest_url = _longest_url
        longest_wc = _longest_word_count
        top_50 = _word_counter.most_common(50)
        subdomains = dict(_subdomain_counter)
        near_dupes = _near_duplicate_count

    return unique_pages, longest_url, longest_wc, top_50, subdomains, near_dupes


def write_report(filepath: str = "report.txt") -> None:
    unique_pages, longest_url, longest_wc, top_50, subdomains, near_dupes = finalize_report()

    lines = []
    lines.append(f"1. Unique pages: {unique_pages}")
    lines.append(f"2. Longest page: {longest_url} ({longest_wc} words)")
    lines.append(f"3. Near-duplicate pages found: {near_dupes}")
    lines.append("4. Top 50 words:")
    for w, c in top_50:
        lines.append(f"   {w}: {c}")
    lines.append("5. Subdomains in uci.edu (alphabetical):")
    for sd in sorted(subdomains.keys()):
        lines.append(f"   {sd}, {subdomains[sd]}")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def print_report() -> None:
    unique_pages, longest_url, longest_wc, top_50, subdomains, near_dupes = finalize_report()

    print("-" * 40)
    print(f"Unique pages: {unique_pages}")
    print(f"Longest Page: {longest_url} ({longest_wc} words)")
    print(f"Near-duplicates: {near_dupes}")
    print("-" * 40)
    print("Top 50 Most Common Words:")
    for w, c in top_50:
        print(f"{w}: {c}")
    print("-" * 40)
    print("Subdomains in uci.edu (ordered alphabetically):")
    for sd in sorted(subdomains.keys()):
        print(f"{sd}, {subdomains[sd]}")
    print("-" * 40)
