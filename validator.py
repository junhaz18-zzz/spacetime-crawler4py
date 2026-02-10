from urllib.parse import urlparse, urldefrag, parse_qs
import re

ALLOWED_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

# Block whole subdomains / host patterns that are clearly low-value for this assignment
# (repos, wikis, infra dashboards, auth portals, mailing lists, etc.)
BLOCKED_HOSTS_EXACT = {
    "gitlab.ics.uci.edu",
    "svn.ics.uci.edu",
    "wiki.ics.uci.edu",
    "swiki.ics.uci.edu",
    "mailman.ics.uci.edu",
    "helpdesk.ics.uci.edu",
    "password.ics.uci.edu",
    "netreg.ics.uci.edu",
    "observium.ics.uci.edu",
    "pgadmin.ics.uci.edu",
    "speedtest.ics.uci.edu",
    "ngs.ics.uci.edu",
}

# In case of variants like git.ics.uci.edu, support.ics.uci.edu, etc.
BLOCKED_HOST_HINTS = (
    "gitlab", "git.", "svn", "wiki", "swiki",
    "mailman", "pipermail",
    "helpdesk", "support",
    "password", "netreg",
    "observium", "pgadmin", "speedtest",
    "intranet", "staging", "archive-beta",
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
    "tribe_bar_date", "tribe_event_display", "eventdate", "start_date", "end_date", "ical",

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
MAX_LIMIT = 100

TRAP_PATH_HINTS = (
    # WP internals / feeds
    "wp-json", "wp-admin", "wp-includes", "wp-content",
    "feed", "rss", "atom", "cgi-bin",

    # Auth/admin/apis
    "login", "logout", "signin", "signout",
    "admin", "api", "graphql",

    # Facets/aggregations (often low-info sets)
    "search", "tag", "tags", "category", "categories",
    "archive", "archives", "author", "authors",

    # Static asset dirs
    "uploads", "assets", "static", "media",

    # Git/code browsing (path-level)
    "tree", "blob", "raw", "blame",
    "commit", "commits", "compare", "diff", "patch",
    "network", "graph", "history",
    "merge_requests", "issues",

    # Low-info auto-generated docs / infra dashboards
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

    # Host-level blocks (biggest crawl-budget savers)
    if host in BLOCKED_HOSTS_EXACT:
        return False
    for hint in BLOCKED_HOST_HINTS:
        if hint in host:
            return False

    path = (parsed.path or "").lower()

    # Block common non-HTML resources by extension
    if any(path.endswith(ext) for ext in BLOCKED_EXTENSIONS):
        return False

    # Avoid extremely long URLs (often traps)
    if len(url) > 300:
        return False

    # Path structure checks
    if has_repeating_path_segments(path):
        return False
    if path_depth(path) > 10:
        return False

    # Block obvious date-archive style paths (e.g., /2023/12/ or /2023-12/)
    if re.search(r"/\d{4}[-/]\d{2}/", path):
        return False

    # Trap-ish path hints
    for hint in TRAP_PATH_HINTS:
        if f"/{hint}/" in path or path.endswith(f"/{hint}"):
            return False

    # Query parameter analysis
    query = parsed.query or ""
    if query:
        qs = parse_qs(query, keep_blank_values=True)
        keys = {k.lower(): k for k in qs.keys()}  # lower -> original

        # WordPress-style filter keys: foo[0]=bar (infinite combinations)
        for k_lower in keys:
            if "[" in k_lower or "]" in k_lower:
                return False

        # Too many params -> combinatorial explosion
        if len(keys) > 4:
            return False

        # Hard block keys
        for k_lower in keys:
            if k_lower in HARD_BLOCK_QUERY_KEYS:
                return False

        # Pagination limits
        if not pagination_within_limits(qs, keys):
            return False

        # Block combinatorial sorting/filtering/view modes (two or more together)
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
    has_page = any(k in keys for k in {"page", "p", "pg", "paged"})
    has_offset = any(k in keys for k in {"start", "offset"})
    # If both styles appear, it's usually a trap
    if has_page and has_offset:
        return False

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

        if k_lower in {"page", "p", "pg", "paged"}:
            if v > MAX_PAGE_NUMBER:
                return False
        elif k_lower in {"start", "offset"}:
            if v > MAX_START_OFFSET:
                return False
        elif k_lower in {"limit", "per_page"}:
            if v > MAX_LIMIT:
                return False

    return True


def has_repeating_path_segments(path: str) -> bool:
    segments = [s for s in path.split("/") if s]
    if len(segments) < 3:
        return False

    for i in range(len(segments) - 2):
        if segments[i] == segments[i + 1] == segments[i + 2]:
            return True

    counts = {}
    for s in segments:
        counts[s] = counts.get(s, 0) + 1
        if counts[s] >= 6:
            return True

    return False


def path_depth(path: str) -> int:
    return len([s for s in path.split("/") if s])
