# validator.py
# Role 2: URL validation + trap avoidance
# Decide if a URL is safe/allowed to be crawled.

from urllib.parse import urlparse, urldefrag, parse_qs
import re

# # Allowed domains with dot (including subdomains)
# ALLOWED_DOMAIN_SUFFIXES = (
#     ".ics.uci.edu",
#     ".cs.uci.edu",
#     ".informatics.uci.edu",
#     ".stat.uci.edu",
# )

# Allowed domains (including subdomains)
# 注意：不带点前缀，这样可以同时匹配 "ics.uci.edu" 和 "www.ics.uci.edu"
ALLOWED_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

# Block common non-HTML resources (extensions)
BLOCKED_EXTENSIONS = (
    # 样式和脚本
    ".css", ".js",
    # 图片
    ".bmp", ".gif", ".jpg", ".jpeg", ".png", ".tiff", ".ico", ".svg", ".webp", ".psd",
    # 音视频
    ".mp2", ".mp3", ".mp4", ".wav", ".avi", ".mov", ".mkv", ".flv", ".wmv", ".ogg", ".webm", ".m4v", ".mpeg", 
    ".ogg", ".ogv", ".webm", ".mid", ".ram", ".rm", ".wma", ".smil", ".swf",
    # 文档
    ".pdf", ".ps", ".eps", ".tex",
    ".ppt", ".pptx", ".doc", ".docx", ".xls", ".xlsx", ".odt", ".ods", ".odp",
    ".names", ".data",
    ".rtf", ".csv", ".arff",
    # 压缩文件
    ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".tgz", ".xz", ".msi", ".exe", ".bin", 
    ".dmg", ".iso", ".apk", ".deb", ".rpm", ".dll", ".cnf",
    # 可执行文件和安装包
    ".exe", ".dmg", ".iso", ".apk", ".deb", ".rpm", ".msi",
    # 数据文件
    ".xml", ".json", ".sql", ".db", ".sqlite",
    # 代码文件（通常不是我们要爬的网页）
    ".py", ".java", ".c", ".cpp", ".h", ".r", ".m", ".mat",
    # 其他
    ".war", ".jar", ".sha1", ".thmx", ".mso",
)

# Trap-related keywords: 完全阻止这些参数
# 注意：分页参数（page, start, offset）已移到单独处理，允许有限制的分页
TRAP_QUERY_KEYS_BLOCK = {
    # 日历/日期相关 - 容易产生无限组合
    "calendar", "cal", "date", "day", "month", "year",
    # 排序/过滤 - 容易产生无限组合
    "sort", "order", "filter", "facet", "action",
    # 追踪参数 - 无用信息
    "replytocom", "share",
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    # 会话参数 - 无用且可能导致重复
    "session", "sid", "phpsessid", "jsessionid",
}

# 分页相关参数 - 允许但有数量限制
PAGINATION_KEYS = {"page", "start", "offset", "p", "pg"}
MAX_PAGE_NUMBER = 20  # page, p, pg 的最大值
MAX_START_OFFSET = 500  # start, offset 的最大值

# Some paths that are often "utility" or "low info / infinite" depending on site
TRAP_PATH_HINTS = (
    "calendar",
    "wp-json",  # WP API endpoints often not human text pages
    "feed",  # RSS feeds
    "cgi-bin",
    "wp-admin",  # WordPress admin
    "wp-includes",  # WordPress system files
    "login",  # Login pages
    "logout",
)


def is_valid(url: str) -> bool:
    """
    Return True iff the given url is allowed and worth crawling.
    Must be within the specified UCI domains and avoid traps/non-html resources.
    """
    if not url or not isinstance(url, str):
        return False

    # Defragment (remove #...); Role1 also does it, but keep safe here.
    url, _frag = urldefrag(url)

    try:
        parsed = urlparse(url)
    except Exception:
        return False

    # 1) Scheme check
    if parsed.scheme not in {"http", "https"}:
        return False

    # 2) Domain check - 修复：同时支持裸域名和子域名
    netloc = parsed.netloc.lower()

    # Remove port if present
    # e.g. "www.ics.uci.edu:443" -> "www.ics.uci.edu"
    host = netloc.split(":")[0]

    if not is_allowed_domain(host):
        return False

    # 3) Path / extension block
    path = (parsed.path or "").lower()

    # Block obvious non-html resource types
    if any(path.endswith(ext) for ext in BLOCKED_EXTENSIONS):
        return False

    # 4) Basic sanity: avoid extremely long URLs (often traps)
    if len(url) > 300:  # 稍微放宽到300
        return False

    # 5) Trap heuristics: repeating directories like /a/a/a/ or /gallery/gallery/gallery
    if has_repeating_path_segments(path):
        return False

    # 6) Trap heuristics: path too deep (often parameterized navigation)
    if path_depth(path) > 10:
        return False

    # 7) Trap heuristics: query string analysis
    query = parsed.query or ""
    if query:
        # Too many parameters -> likely filters/search pages generating infinite combinations
        if query_param_count(query) > 8:
            return False

        # Parse query keys
        qs = parse_qs(query, keep_blank_values = True)
        keys_lower = {k.lower(): k for k in qs.keys()}  # 保存原始key的映射

        # 7a) 完全阻止的参数
        if any(k in TRAP_QUERY_KEYS_BLOCK for k in keys_lower):
            return False

        # 7b) 分页参数：允许但有限制
        if not is_pagination_allowed(qs, keys_lower):
            return False

    # 8) Trap heuristics: suspicious path hints
    for hint in TRAP_PATH_HINTS:
        if f"/{hint}/" in path or path.endswith(f"/{hint}"):
            return False

    return True


def is_allowed_domain(host: str) -> bool:
    """
    检查 host 是否是允许的域名。
    支持：
      - 精确匹配：ics.uci.edu
      - 子域名匹配：www.ics.uci.edu, vision.ics.uci.edu
    """
    for domain in ALLOWED_DOMAINS:
        # 精确匹配
        if host == domain:
            return True
        # 子域名匹配（以 .domain 结尾）
        if host.endswith("." + domain):
            return True
    return False


def is_pagination_allowed(qs: dict, keys_lower: dict) -> bool:
    """
    检查分页参数是否在允许范围内。
    返回 True 表示允许，False 表示阻止。
    """
    for key_lower in keys_lower:
        if key_lower in PAGINATION_KEYS:
            original_key = keys_lower[key_lower]
            values = qs.get(original_key, [])

            if not values:
                continue

            try:
                value = int(values[0])

                # page, p, pg 使用页数限制
                if key_lower in {"page", "p", "pg"}:
                    if value > MAX_PAGE_NUMBER:
                        return False
                # start, offset 使用偏移量限制
                elif key_lower in {"start", "offset"}:
                    if value > MAX_START_OFFSET:
                        return False

            except (ValueError, TypeError):
                # 如果不是数字，可能是trap，阻止
                return False

    return True


def has_repeating_path_segments(path: str) -> bool:
    """
    Detect repeating directory segments that suggest a trap.
    Examples:
      /gallery/gallery/gallery/
      /events/2025/01/events/2025/01/ ...
    We check:
      - immediate triple repetition (x/x/x)
      - high repetition count of same segment
    """
    segments = [s for s in path.split("/") if s]
    if len(segments) < 3:
        return False

    # Rule A: immediate triple repetition
    for i in range(len(segments) - 2):
        if segments[i] == segments[i + 1] == segments[i + 2]:
            return True

    # Rule B: any segment appears too many times overall
    counts = {}
    for s in segments:
        counts[s] = counts.get(s, 0) + 1
        if counts[s] >= 6:
            return True

    return False


def path_depth(path: str) -> int:
    """返回路径深度（目录层数）"""
    segments = [s for s in path.split("/") if s]
    return len(segments)


def query_param_count(query: str) -> int:
    """计算查询参数数量"""
    return query.count("&") + 1 if query else 0


