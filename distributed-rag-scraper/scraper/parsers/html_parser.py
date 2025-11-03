"""
HTML Parser using BeautifulSoup
Extracts clean text and structured data from raw HTML
"""
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Optional


class HTMLParser:
    """
    Parse HTML content and extract structured information
    """
    
    def __init__(self):
        """Initialize HTML parser"""
        self.unwanted_tags = [
            'script', 'style', 'nav', 'footer', 'header',
            'aside', 'iframe', 'noscript', 'svg', 'canvas'
        ]
        self.unwanted_classes = [
            'advertisement', 'ads', 'ad-banner', 'popup',
            'cookie-notice', 'social-share', 'sidebar'
        ]
    
    def parse(self, html: str, url: str = '') -> Dict:
        """
        Parse HTML and extract structured data
        
        Args:
            html: Raw HTML content
            url: Source URL (for context)
            
        Returns:
            dict: Parsed and structured data
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Remove unwanted elements
        self._remove_unwanted_elements(soup)
        
        # Extract structured data
        parsed_data = {
            'url': url,
            'title': self._extract_title(soup),
            'headings': self._extract_headings(soup),
            'paragraphs': self._extract_paragraphs(soup),
            'lists': self._extract_lists(soup),
            'links': self._extract_links(soup, url),
            'images': self._extract_images(soup),
            'metadata': self._extract_metadata(soup),
            'main_content': self._extract_main_content(soup),
            'clean_text': self._extract_clean_text(soup),
            'word_count': 0,
            'language': self._detect_language(soup),
        }
        
        # Calculate word count
        parsed_data['word_count'] = len(parsed_data['clean_text'].split())
        
        return parsed_data
    
    def _remove_unwanted_elements(self, soup: BeautifulSoup) -> None:
        """Remove unwanted HTML elements"""
        # Remove by tag name
        for tag in self.unwanted_tags:
            for element in soup.find_all(tag):
                element.decompose()
        
        # Remove by class name
        for class_name in self.unwanted_classes:
            for element in soup.find_all(class_=re.compile(class_name, re.I)):
                element.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, str) and text.strip().startswith('<!--')):
            comment.extract()
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title"""
        # Try <title> tag
        title = soup.find('title')
        if title:
            return title.get_text().strip()
        
        # Try <h1> tag
        h1 = soup.find('h1')
        if h1:
            return h1.get_text().strip()
        
        return ''
    
    def _extract_headings(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract all headings (h1-h6)"""
        headings = {}
        for level in range(1, 7):
            tag = f'h{level}'
            headings[tag] = [
                h.get_text().strip()
                for h in soup.find_all(tag)
                if h.get_text().strip()
            ]
        return headings
    
    def _extract_paragraphs(self, soup: BeautifulSoup) -> List[str]:
        """Extract all paragraph text"""
        paragraphs = []
        for p in soup.find_all('p'):
            text = p.get_text().strip()
            if text and len(text) > 20:  # Filter out very short paragraphs
                paragraphs.append(text)
        return paragraphs
    
    def _extract_lists(self, soup: BeautifulSoup) -> Dict[str, List[List[str]]]:
        """Extract ordered and unordered lists"""
        lists = {
            'ordered': [],
            'unordered': []
        }
        
        # Extract unordered lists
        for ul in soup.find_all('ul'):
            items = [li.get_text().strip() for li in ul.find_all('li', recursive=False)]
            if items:
                lists['unordered'].append(items)
        
        # Extract ordered lists
        for ol in soup.find_all('ol'):
            items = [li.get_text().strip() for li in ol.find_all('li', recursive=False)]
            if items:
                lists['ordered'].append(items)
        
        return lists
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
        """Extract all links with anchor text"""
        links = []
        seen_urls = set()
        
        for a in soup.find_all('a', href=True):
            url = a['href']
            text = a.get_text().strip()
            
            # Skip empty or duplicate links
            if not url or url in seen_urls or url.startswith(('#', 'javascript:', 'mailto:')):
                continue
            
            seen_urls.add(url)
            links.append({
                'url': url,
                'text': text,
                'title': a.get('title', '')
            })
        
        return links[:100]  # Limit to first 100 links
    
    def _extract_images(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract image information"""
        images = []
        
        for img in soup.find_all('img'):
            src = img.get('src', '')
            if src:
                images.append({
                    'src': src,
                    'alt': img.get('alt', ''),
                    'title': img.get('title', '')
                })
        
        return images[:50]  # Limit to first 50 images
    
    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract meta tags"""
        metadata = {}
        
        # Open Graph tags
        for tag in soup.find_all('meta', property=re.compile('^og:')):
            prop = tag.get('property', '')
            content = tag.get('content', '')
            if prop and content:
                metadata[prop] = content
        
        # Standard meta tags
        for tag in soup.find_all('meta', attrs={'name': True}):
            name = tag.get('name', '')
            content = tag.get('content', '')
            if name and content:
                metadata[name] = content
        
        return metadata
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """
        Extract main content area
        Looks for common content containers
        """
        # Try common content containers
        content_selectors = [
            'main', 'article', '[role="main"]',
            '.content', '.main-content', '#content',
            '.post-content', '.entry-content'
        ]
        
        for selector in content_selectors:
            content = soup.select_one(selector)
            if content:
                return content.get_text(separator=' ', strip=True)
        
        # Fallback: get body text
        body = soup.find('body')
        if body:
            return body.get_text(separator=' ', strip=True)
        
        return ''
    
    def _extract_clean_text(self, soup: BeautifulSoup) -> str:
        """
        Extract all visible text, cleaned and normalized
        """
        # Get all text
        text = soup.get_text(separator=' ', strip=True)
        
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def _detect_language(self, soup: BeautifulSoup) -> str:
        """Detect page language"""
        # Check html lang attribute
        html_tag = soup.find('html')
        if html_tag and html_tag.get('lang'):
            return html_tag['lang']
        
        # Check meta tags
        lang_meta = soup.find('meta', attrs={'http-equiv': 'content-language'})
        if lang_meta and lang_meta.get('content'):
            return lang_meta['content']
        
        return 'en'  # Default to English


def parse_html(html: str, url: str = '') -> Dict:
    """
    Convenience function to parse HTML
    
    Args:
        html: Raw HTML content
        url: Source URL
        
    Returns:
        dict: Parsed data
    """
    parser = HTMLParser()
    return parser.parse(html, url)