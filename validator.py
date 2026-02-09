from urllib.parse import urlparse, urldefrag, parse_qs
import re

ALLOWED_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

BLOCKED_EXTENSIONS = (
    # Assets & Media
    ".css", ".js", ".mjs", ".map", ".wasm",
    ".bmp", ".gif", ".jpg", ".jpeg", ".png", ".tiff", ".tif", ".ico", ".svg", ".webp",
    ".psd", ".ai", ".eps", ".heic", ".heif", ".avif", ".jp2",
    ".mp2", ".mp3", ".m4a", ".aac", ".flac", ".wav", ".wma", ".aiff", ".au",
    ".mp4", ".m4v", ".mov", ".avi", ".mkv", ".flv", ".wmv", ".webm", ".mpeg", ".mpg",
    ".ogv", ".ogg", ".m3u8", ".ts", ".srt", ".vtt",

    # Documents & Fonts
    ".pdf", ".ps", ".tex", ".djvu",
    ".ppt", ".pptx", ".pptm", ".pps", ".ppsx", ".ppsm", ".pot", ".potx", ".potm",
    ".doc", ".docx", ".docm", ".xls", ".xlsx", ".xlsm", ".odt", ".ods", ".odp",
    ".rtf", ".txt", ".epub", ".mobi", ".azw", ".azw3",
    ".woff", ".woff2", ".ttf", ".eot", ".otf",

    # Data, Logs, Archives, Executables
    ".xml", ".json", ".jsonl", ".ndjson", ".yaml", ".yml", ".toml",
    ".sql", ".db", ".sqlite", ".sqlite3", ".csv", ".tsv",
    ".log", ".dat", ".bak", ".tmp", ".swp", ".old", ".dmp", ".dump",
    ".zip", ".rar", ".7z", ".tar", ".tgz", ".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst",
    ".gz", ".bz2", ".xz", ".zst", ".lz4", ".iso", ".img",
    ".exe", ".msi", ".bin", ".dll", ".so", ".dylib", ".deb", ".rpm", ".apk", ".dmg", ".pkg", ".cab",
    ".jar", ".war", ".ear", ".class",

    # Source Code & Configs
    ".c", ".cc", ".cpp", ".cxx", ".h", ".hpp",
    ".java", ".py", ".ipynb",
    ".sh", ".bash", ".zsh", ".ps1", ".bat", ".cmd",
    ".go", ".rs", ".rb", ".php", ".pl", ".swift", ".kt",
    ".m", ".mat", ".r",
    ".ini", ".cfg", ".conf", ".cnf", ".env", ".pem", ".crt", ".cer", ".key",

    # Traps & Feeds
    ".ics", ".rss", ".atom", ".arff", ".diff", ".patch",
)

HARD_BLOCK_QUERY_KEYS = {
    # Calendar / Date Traps
    "day", "month", "year", "date", "time",
    "tribe_bar_date", "tribe_event_display", "eventDate", "start_date", "end_date", "ical",

    # Functional / Low Info Pages
    "print", "printable", "download", "attachment", "preview", 
    "fullscreen", "mobile", "view_mode",
    "diff", "oldid", "action", "mode",

    # Tracking & Session
    "replytocom", "share", "shared", "share_id",
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "dclid", "gbraid", "wbraid", "fbclid", "msclkid", "mc_cid", "mc_eid", "igshid", "yclid",
    "ref", "ref_", "referrer", "source", "src", "campaign", "adid",
    "session", "sid", "phpsessid", "jsessionid", "state",

    # Cache-bust & Auth
    "_", "_t", "cb", "cachebust", "nocache", "timestamp", "ts", "rnd", "random",
    "v", "ver", "version", "hash",
    "token", "access_token", "auth", "oauth", "apikey", "key", "signature", "sig", "expires",
    "samlrequest", "samlresponse",

    # Site-specific & Sort
    "do", "rev", "image", "tab_files", "tab_details",
    "sort", "order",
}

PAGINATION_KEYS = {"page", "p", "pg", "paged", "start", "offset", "limit", "per_page"}
MAX_PAGE_NUMBER = 20
MAX_START_OFFSET = 500

TRAP_PATH_HINTS = (
    "wp-json", "wp-admin", "wp-includes", "wp-content",
    "feed", "rss", "atom", "cgi-bin",
    "login", "logout", "signin", "signout",
    "admin", "api", "graphql",
    "search", "tag", "tags", "category", "categories",
    "archive", "archives", "author", "authors",
    "uploads", "assets", "static", "media",
    
    # GitLab / Code Browsing Traps
    "tree", "blob", "commit", "commits", "compare", "network", "graph",
    
    # Calendar / Infinite Date Traps
    "calendar", "events", "agenda", "schedule", "bitstream", "retrieve",
    
    # Low Info / Auto-generated Docs
    "mailman", "pipermail", "javadoc", "doxygen", "epydoc", "apidocs",
    "ganglia", "nagios", "mrtg",
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

    # --- THE NUCLEAR OPTION ---
    # Explicitly block GitLab. This kills the entire site tree.
    if "gitlab" in host:
        return False
    # --------------------------

    path = (parsed.path or "").lower()

    if any(path.endswith(ext) for ext in BLOCKED_EXTENSIONS):
        return False

    if len(url) > 300:
        return False

    # Path structure checks
    if has_repeating_path_segments(path):
        return False
    if path_depth(path) > 10:
        return False
    
    # --- Block date-based archive/calendar paths ---
    # Detects patterns like /2023/12/ or /2023-12/
    if re.search(r"/\d{4}[-/]\d{2}/", path):
        return False

    for hint in TRAP_PATH_HINTS:
        if f"/{hint}/" in path or path.endswith(f"/{hint}"):
            return False

    # Query parameter analysis
    query = parsed.query or ""
    if query:
        qs = parse_qs(query, keep_blank_values=True)
        keys = {k.lower(): k for k in qs.keys()}

        for k in keys:
            if "[" in k or "]" in k:
                return False

        if len(keys) > 4:
            return False

        for k in keys:
            if k in HARD_BLOCK_QUERY_KEYS:
                return False

        if not pagination_within_limits(qs, keys):
            return False

        # Block combinatorial sorting/filtering/viewing
        combinatorial = {"sort", "order", "filter", "facet", "action", "view", "layout"}
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
        else:
            if v > MAX_START_OFFSET:
                return False
    return True


def has_repeating_path_segments(path: str) -> bool:
    segments = [s for s in path.split("/") if s]
    if len(segments) < 3:
        return False

    # Check immediate triple repetition
    for i in range(len(segments) - 2):
        if segments[i] == segments[i + 1] == segments[i + 2]:
            return True

    # Check total frequency of any segment
    counts = {}
    for s in segments:
        counts[s] = counts.get(s, 0) + 1
        if counts[s] >= 6:
            return True
    return False


def path_depth(path: str) -> int:
    return len([s for s in path.split("/") if s])