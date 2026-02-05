from urllib.parse import urlparse, urldefrag, parse_qs
import re

ALLOWED_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

# Block obvious non-HTML resources (extensions)
BLOCKED_EXTENSIONS = (
    ".css", ".js",
    ".bmp", ".gif", ".jpg", ".jpeg", ".png", ".tiff", ".ico", ".svg", ".webp", ".psd",
    ".mp2", ".mp3", ".mp4", ".wav", ".avi", ".mov", ".mkv", ".flv", ".wmv",
    ".ogg", ".ogv", ".webm", ".m4v", ".mpeg",
    ".mid", ".ram", ".rm", ".wma", ".smil", ".swf",
    ".pdf", ".ps", ".eps", ".tex",
    ".ppt", ".pptx", ".doc", ".docx", ".xls", ".xlsx", ".odt", ".ods", ".odp",
    ".rtf", ".csv", ".arff",
    ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".tgz", ".xz",
    ".msi", ".exe", ".bin", ".dmg", ".iso", ".apk", ".deb", ".rpm", ".dll", ".woff", ".woff2", ".ttf", ".eot", ".otf", 
    ".ics", ".rss", ".atom", ".xml", ".json", ".yaml", ".yml", ".sql", ".db", ".sqlite", ".log", ".dat", 
    ".c", ".cpp", ".h", ".hpp", ".java", ".py", ".sh", ".bash", ".ipynb", 
    ".jar", ".war", ".class", ".bak", ".tmp", ".swp"
)

# Hard-block tracking/session keys (almost always useless duplicates)
HARD_BLOCK_QUERY_KEYS = {
    "replytocom", "share",
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "session", "sid", "phpsessid", "jsessionid",
    "do",
    "rev",
    "image",     
    "tab_files",  
    "tab_details", 
    # List sorting
    "sort", "order"
}

# Pagination keys: allow within limits
PAGINATION_KEYS = {"page", "p", "pg", "start", "offset"}
MAX_PAGE_NUMBER = 20
MAX_START_OFFSET = 500

# Path hints that are often not useful / trap-ish
TRAP_PATH_HINTS = (
    "wp-json",
    "feed",
    "cgi-bin",
    "wp-admin",
    "wp-includes",
    "login",
    "logout",
)

def is_valid(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False

    url, _ = urldefrag(url)

    try:
        parsed = urlparse(url)
    except Exception:
        return False

    if parsed.scheme not in {"http", "https"}:
        return False

    host = (parsed.netloc or "").lower().split(":")[0]
    if not is_allowed_domain(host):
        return False

    path = (parsed.path or "").lower()

    # Block obvious non-html by extension
    if any(path.endswith(ext) for ext in BLOCKED_EXTENSIONS):
        return False

    # Avoid extremely long URLs (often traps)
    if len(url) > 300:
        return False

    # Repeating segments / too deep paths
    if has_repeating_path_segments(path):
        return False
    if path_depth(path) > 10:
        return False

    # Trap-ish path hints
    for hint in TRAP_PATH_HINTS:
        if f"/{hint}/" in path or path.endswith(f"/{hint}"):
            return False

    # Query handling (less aggressive than your current version)
    query = parsed.query or ""
    if query:
        qs = parse_qs(query, keep_blank_values=True)
        keys = {k.lower(): k for k in qs.keys()}

        # too many params -> likely infinite combinations
        if len(keys) > 8:
            return False

        # Hard-block tracking/session keys
        for k in keys:
            if k in HARD_BLOCK_QUERY_KEYS:
                return False

        # Pagination limits
        if not pagination_within_limits(qs, keys):
            return False

        # Soft rules: allow a small amount of sort/order/filter/action,
        # but block if multiple "combinatorial" keys appear together.
        combinatorial = {"sort", "order", "filter", "facet", "action"}
        present = [k for k in keys if k in combinatorial]
        if len(present) >= 2:
            return False

    return True

def is_allowed_domain(host: str) -> bool:
    for domain in ALLOWED_DOMAINS:
        if host == domain:
            return True
        if host.endswith("." + domain):
            return True
    return False

def pagination_within_limits(qs: dict, keys: dict) -> bool:
    for k_lower, original_key in keys.items():
        if k_lower not in PAGINATION_KEYS:
            continue
        values = qs.get(original_key, [])
        if not values:
            continue
        try:
            v = int(values[0])
        except Exception:
            return False

        if k_lower in {"page", "p", "pg"}:
            if v > MAX_PAGE_NUMBER:
                return False
        else:  # start / offset
            if v > MAX_START_OFFSET:
                return False
    return True

def has_repeating_path_segments(path: str) -> bool:
    segments = [s for s in path.split("/") if s]
    if len(segments) < 3:
        return False

    # immediate triple repetition
    for i in range(len(segments) - 2):
        if segments[i] == segments[i + 1] == segments[i + 2]:
            return True

    # any segment repeats too many times
    counts = {}
    for s in segments:
        counts[s] = counts.get(s, 0) + 1
        if counts[s] >= 6:
            return True
    return False

def path_depth(path: str) -> int:
    return len([s for s in path.split("/") if s])
