import re
from urllib.parse import urlparse, urljoin
from html.parser import HTMLParser

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200:
        return list()
    if not resp.raw_response or not resp.raw_response.content:
        return list()
    try:
        html_content = resp.raw_response.content.decode('utf-8') # convert byte to string for HTML parsing
    except Exception:
        return list()
    
    parser = LinkParser(resp.url) #passing resp.url ensures relative links are calculated from the correct folder
    try:
        parser.feed(html_content)
    except Exception: # for malformed html
        return list()
    return parser.output_links

class LinkParser(HTMLParser):
    """Helper class for parsing HTML Link"""
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.output_links = []

    def handle_starttag(self, tag, attr):
        if tag != 'a': # only tag <a> contains link
            return
        attributes = dict(attr)
        # Convert list of tuples [('href', 'url')...] to dict {'href': 'url'}
        href = attributes.get('href')
        if href:
            clean_link = self._clean_url(href)
            if clean_link:
                self.output_links.append(clean_link)
    
    def _clean_url(self, raw_url):
        """Helper to normalize URL"""
        absolute_url = urljoin(self.base_url, raw_url)
        parsed = urlparse(absolute_url)
        # Removes the framgement (#) of a URL.
        return parsed._replace(fragment="").geturl()

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise


