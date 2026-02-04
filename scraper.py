import re
from urllib.parse import urlparse, urljoin
from html.parser import HTMLParser
from validator import is_valid

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
    # Defensive check for neglected files (>10MB)
    if len(resp.raw_response.content) > 10 * 1024 * 1024:
        return list()
    
    try:
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
        extracted_links = set() # eliminate duplicates
    
        for link in soup.find_all('a', href=True):
            lianjie = link['href']
            base_url = resp.url if resp.url else url
            absolute_url = urljoin(base_url, lianjie) # Handles redirects correctly using resp.url
            
            # Remove #fragment
            clean_url = urlparse(absolute_url)._replace(fragment="").geturl() 
            extracted_links.add(clean_url)
        return list(extracted_links)   
    except Exception:
        # If lxml crashes or content is binary trash
        return list()
