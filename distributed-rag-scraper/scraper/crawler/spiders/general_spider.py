"""
General Web Spider for Distributed RAG Scraper
Crawls websites and extracts content for processing
"""
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from datetime import datetime
import hashlib


class GeneralSpider(CrawlSpider):
    """
    General purpose web spider that follows links and extracts content
    """
    name = 'general'
    
    # Custom settings for this spider
    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 1,
        'DEPTH_LIMIT': 3,  # Maximum crawl depth
        'ROBOTSTXT_OBEY': True,
    }
    
    # Define rules for following links
    rules = (
        Rule(
            LinkExtractor(
                allow=(),  # Regex patterns to allow
                deny=('logout', 'login', 'signup'),  # Patterns to avoid
                deny_extensions=('pdf', 'zip', 'tar', 'gz', 'jpg', 'png', 'gif')
            ),
            callback='parse_page',
            follow=True
        ),
    )
    
    def __init__(self, *args, **kwargs):
        """Initialize spider with start URLs"""
        super(GeneralSpider, self).__init__(*args, **kwargs)
        
        # Start URLs - can be passed as argument
        self.start_urls = kwargs.get('start_urls', [
            'https://quotes.toscrape.com/',  # Example site for testing
        ])
        
        # Convert string to list if needed
        if isinstance(self.start_urls, str):
            self.start_urls = [url.strip() for url in self.start_urls.split(',')]
    
    def parse_page(self, response):
        """
        Parse each crawled page and extract data
        
        Args:
            response: Scrapy response object
            
        Yields:
            dict: Extracted page data
        """
        # Generate unique ID for this page
        url_hash = hashlib.md5(response.url.encode()).hexdigest()
        
        # Extract metadata
        page_data = {
            'id': url_hash,
            'url': response.url,
            'title': self._extract_title(response),
            'html': response.text,
            'status_code': response.status,
            'headers': {k.decode('utf-8'): [v.decode('utf-8') for v in vals] 
                       for k, vals in response.headers.items()},
            'timestamp': datetime.utcnow().isoformat(),
            'depth': response.meta.get('depth', 0),
            'links': self._extract_links(response),
            'meta_description': self._extract_meta_description(response),
            'meta_keywords': self._extract_meta_keywords(response),
        }
        
        self.logger.info(f"Scraped: {response.url} (depth: {page_data['depth']})")
        
        yield page_data
    
    def _extract_title(self, response):
        """Extract page title"""
        title = response.css('title::text').get()
        if not title:
            title = response.xpath('//title/text()').get()
        return title.strip() if title else ''
    
    def _extract_meta_description(self, response):
        """Extract meta description"""
        desc = response.css('meta[name="description"]::attr(content)').get()
        if not desc:
            desc = response.xpath('//meta[@name="description"]/@content').get()
        return desc.strip() if desc else ''
    
    def _extract_meta_keywords(self, response):
        """Extract meta keywords"""
        keywords = response.css('meta[name="keywords"]::attr(content)').get()
        if not keywords:
            keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        return keywords.strip() if keywords else ''
    
    def _extract_links(self, response):
        """Extract all links from the page"""
        links = response.css('a::attr(href)').getall()
        # Convert relative URLs to absolute
        absolute_links = [response.urljoin(link) for link in links]
        return absolute_links[:50]  # Limit to first 50 links
    
    def parse_start_url(self, response):
        """Parse the start URLs"""
        return self.parse_page(response)