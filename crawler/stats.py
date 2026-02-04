from threading import RLock
from collections import Counter
from urllib.parse import urlparse

# Import the logic from your analytics.py file
from analytics import tokenize, compute_simhash, is_near_duplicate

class Stats:
    def __init__(self):
        self.lock = RLock()
        self.subdomains = Counter()
        self.word_counts = Counter()
        self.longest_page = {"url": None, "count": 0}
        self.seen_hashes = [] # Stores 64-bit fingerprints to detect duplicates

    def is_duplicate(self, text):
        """
        Checks if the page content is an exact or near duplicate of a page we have already seen.
        Uses SimHash + Hamming Distance (as required by the assignment).
        """
        new_hash = compute_simhash(text)
        
        with self.lock:
            # Compare against all history
            for seen_hash in self.seen_hashes:
                if is_near_duplicate(new_hash, seen_hash):
                    return True # It is a duplicate
            
            # If it's unique, save the fingerprint and return False
            self.seen_hashes.append(new_hash)
            return False

    def add_page(self, url, text):
        """
        Updates the global statistics with data from a new page.
        """
        # 1. Tokenize (using the optimized regex in analytics.py)
        words = tokenize(text)
        word_count = len(words)

        with self.lock:
            # Update Word Frequencies
            self.word_counts.update(words)

            # Update Longest Page
            if word_count > self.longest_page["count"]:
                self.longest_page = {"url": url, "count": word_count}

            # Update Subdomains (using .hostname to ignore ports)
            parsed = urlparse(url)
            hostname = parsed.hostname 
            if hostname and "uci.edu" in hostname.lower():
                self.subdomains[hostname.lower()] += 1

    def dump_report(self):
        """Prints the final analytics report."""
        print("\n" + "=" * 40)
        print(f"Longest Page: {self.longest_page['url']} ({self.longest_page['count']} words)")
        print("-" * 40)
        
        print("Top 50 Most Common Words:")
        for word, count in self.word_counts.most_common(50):
            print(f"{word}: {count}")
        print("-" * 40)
        
        print("Subdomains in uci.edu:")
        for subdomain, count in sorted(self.subdomains.items()):
            print(f"{subdomain}, {count}")
        print("=" * 40)