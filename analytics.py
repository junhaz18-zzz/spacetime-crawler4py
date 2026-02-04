import re
import hashlib
from collections import Counter
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# Standard list of English stop words to ignore.
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

TOKEN_PATTERN = re.compile(r"[a-z]+(?:'[a-z]+)?")

def extract_text_from_html(html_content):
    """
    Strips HTML tags to return human-readable text.
    HTML markup does not count as words.
    """
    if not html_content:
        return ""
    
    # Use lxml builder for performance
    try:
        soup = BeautifulSoup(html_content, 'lxml')
    except Exception:
        soup = BeautifulSoup(html_content, 'html.parser')
    
    text = soup.get_text(separator=' ', strip=True)
    return text

def compute_simhash(text):
    """
    Computes a 64-bit SimHash fingerprint for the given text.
    """
    # Tokenize
    tokens = TOKEN_PATTERN.findall(text.lower())
    
    if not tokens:
        return 0
        
    # Initialize vector v to 0
    v = [0] * 64
    
    for token in tokens:
        # Hash the token using MD5
        # Get 64-bit hash 
        token_hash = int(hashlib.md5(token.encode('utf-8')).hexdigest()[:16], 16)
        
        # Update vector
        for i in range(64):
            bit = (token_hash >> i) & 1
            if bit == 1:
                v[i] += 1
            else:
                v[i] -= 1
                
    # Build the fingerprint
    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= (1 << i)
            
    return fingerprint

def is_near_duplicate(simhash1, simhash2, threshold=2):
    """
    Checks if two pages are near duplicates based on Hamming Distance.
    """
    x = simhash1 ^ simhash2
    
    # Count the number of 1s 
    distance = bin(x).count('1')
    
    return distance <= threshold

def tokenize(text):
    """
    Tokenizes text into words, handling contractions and ignoring stop words.
    """
    words = TOKEN_PATTERN.findall(text.lower())
    
    # Filter out stop words and short tokens
    return [w for w in words if w not in STOP_WORDS and len(w) > 1]

def get_word_frequencies(text_content_list):
    """
    DEPRECATED FOR LARGE CRAWLS: Only used if running analytics on a small list.
    For the main crawler, the Stats class handles aggregation.
    """
    counter = Counter()
    for text in text_content_list:
        words = tokenize(text)
        counter.update(words)
    return counter.most_common(50)

def track_subdomains(visited_urls):
    """
    Tracks how many unique URLs belong to each subdomain in the uci.edu domain.
    """
    subdomain_counts = Counter()

    for url in visited_urls:
        parsed_url = urlparse(url)
        
        # Use hostname instead of netloc to automatically strip ports 
        hostname = parsed_url.hostname
        
        # Check if the hostname is within uci.edu
        if hostname and "uci.edu" in hostname.lower():
            subdomain_counts[hostname.lower()] += 1
            
    # Return the dictionary
    return dict(subdomain_counts)

def print_analytics_report(longest_page_url, longest_page_word_count, top_50_words, subdomain_stats):
    """
    Helper function to print the analytics report.
    """
    print("-" * 40)
    print(f"Longest Page: {longest_page_url} ({longest_page_word_count} words)")
    print("-" * 40)
    
    print("Top 50 Most Common Words:")
    for word, count in top_50_words:
        print(f"{word}: {count}")
    print("-" * 40)
    
    print("Subdomains in uci.edu (ordered alphabetically):")
    for subdomain in sorted(subdomain_stats.keys()):
        print(f"{subdomain}, {subdomain_stats[subdomain]}")
    print("-" * 40)