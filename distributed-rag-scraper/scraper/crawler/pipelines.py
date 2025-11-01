"""
Scrapy pipelines for data processing and storage
"""
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime


class ValidationPipeline:
    """
    Validate scraped items before storage
    """
    
    def process_item(self, item, spider):
        """
        Validate item fields
        
        Args:
            item: Scraped item
            spider: Spider instance
            
        Returns:
            dict: Validated item
        """
        # Required fields
        required_fields = ['url', 'html', 'timestamp']
        
        for field in required_fields:
            if field not in item or not item[field]:
                spider.logger.warning(f"Missing required field: {field} for URL: {item.get('url', 'unknown')}")
                raise ValueError(f"Missing required field: {field}")
        
        # Clean and validate URL
        if not item['url'].startswith(('http://', 'https://')):
            raise ValueError(f"Invalid URL: {item['url']}")
        
        # Ensure HTML is not empty
        if len(item['html']) < 100:
            spider.logger.warning(f"HTML content too short for URL: {item['url']}")
        
        # Add processing metadata
        item['processed'] = False
        item['scrape_timestamp'] = datetime.utcnow()
        
        return item


class MongoDBPipeline:
    """
    Store scraped items in MongoDB
    """
    
    def __init__(self, mongo_uri, mongo_db, collection_name):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @classmethod
    def from_crawler(cls, crawler):
        """
        Initialize pipeline from crawler settings
        
        Args:
            crawler: Scrapy crawler
            
        Returns:
            MongoDBPipeline: Pipeline instance
        """
        return cls(
            mongo_uri=crawler.settings.get('MONGODB_URI'),
            mongo_db=crawler.settings.get('MONGODB_DATABASE'),
            collection_name=crawler.settings.get('MONGODB_COLLECTION', 'raw_data')
        )
    
    def open_spider(self, spider):
        """
        Initialize MongoDB connection when spider opens
        
        Args:
            spider: Spider instance
        """
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            self.collection = self.db[self.collection_name]
            
            # Create index on URL for faster lookups and prevent duplicates
            self.collection.create_index('url', unique=True)
            self.collection.create_index('timestamp')
            self.collection.create_index('processed')
            
            spider.logger.info(f"Connected to MongoDB: {self.mongo_db}.{self.collection_name}")
            
        except Exception as e:
            spider.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def close_spider(self, spider):
        """
        Close MongoDB connection when spider closes
        
        Args:
            spider: Spider instance
        """
        if self.client:
            self.client.close()
            spider.logger.info("Closed MongoDB connection")
    
    def process_item(self, item, spider):
        """
        Save item to MongoDB
        
        Args:
            item: Scraped item
            spider: Spider instance
            
        Returns:
            dict: Processed item
        """
        try:
            # Convert item to dict if needed
            item_dict = dict(item)
            
            # Insert into MongoDB
            result = self.collection.insert_one(item_dict)
            spider.logger.info(f"Saved to MongoDB: {item['url']} (ID: {result.inserted_id})")
            
            # Add MongoDB ID to item
            item['_id'] = str(result.inserted_id)
            
        except DuplicateKeyError:
            spider.logger.warning(f"Duplicate URL skipped: {item['url']}")
            
        except Exception as e:
            spider.logger.error(f"Error saving to MongoDB: {e}")
            raise
        
        return item


class DataCleaningPipeline:
    """
    Clean and normalize scraped data
    """
    
    def process_item(self, item, spider):
        """
        Clean item data
        
        Args:
            item: Scraped item
            spider: Spider instance
            
        Returns:
            dict: Cleaned item
        """
        # Remove extra whitespace from title
        if 'title' in item and item['title']:
            item['title'] = ' '.join(item['title'].split())
        
        # Clean meta description
        if 'meta_description' in item and item['meta_description']:
            item['meta_description'] = ' '.join(item['meta_description'].split())
        
        # Limit number of links stored
        if 'links' in item and isinstance(item['links'], list):
            item['links'] = item['links'][:100]  # Keep first 100 links
        
        # Add content length
        if 'html' in item:
            item['content_length'] = len(item['html'])
        
        return item