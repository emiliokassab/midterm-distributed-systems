"""
Process raw HTML data from MongoDB and save cleaned/parsed data
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient
from scraper.parsers.html_parser import parse_html
from scraper.parsers.text_cleaner import clean_text
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HTMLProcessor:
    """
    Process raw HTML from MongoDB
    """
    
    def __init__(self, mongo_uri='mongodb://localhost:27017', db_name='rag_scraper'):
        """Initialize processor"""
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.raw_collection = self.db['raw_data']
        self.processed_collection = self.db['processed_data']
        
        # Create indexes
        self.processed_collection.create_index('url', unique=True)
        self.processed_collection.create_index('processed_at')
        
        logger.info(f"Connected to MongoDB: {db_name}")
    
    def process_all_unprocessed(self, limit=None):
        """
        Process all unprocessed documents
        
        Args:
            limit: Maximum number of documents to process
        """
        # Find unprocessed documents
        query = {'processed': False}
        
        if limit:
            cursor = self.raw_collection.find(query).limit(limit)
        else:
            cursor = self.raw_collection.find(query)
        
        total = self.raw_collection.count_documents(query)
        logger.info(f"Found {total} unprocessed documents")
        
        processed_count = 0
        error_count = 0
        
        for raw_doc in cursor:
            try:
                # Process document
                processed_doc = self.process_document(raw_doc)
                
                # Save to processed collection
                self.save_processed(processed_doc)
                
                # Mark as processed in raw collection
                self.raw_collection.update_one(
                    {'_id': raw_doc['_id']},
                    {'$set': {'processed': True}}
                )
                
                processed_count += 1
                logger.info(f"Processed [{processed_count}/{total}]: {raw_doc['url']}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing {raw_doc.get('url', 'unknown')}: {e}")
        
        logger.info(f"Processing complete: {processed_count} successful, {error_count} errors")
    
    def process_document(self, raw_doc):
        """
        Process a single raw document
        
        Args:
            raw_doc: Raw document from MongoDB
            
        Returns:
            dict: Processed document
        """
        url = raw_doc.get('url', '')
        html = raw_doc.get('html', '')
        
        # Parse HTML
        parsed_data = parse_html(html, url)
        
        # Clean text
        parsed_data['clean_text'] = clean_text(parsed_data['clean_text'])
        
        # Add original metadata
        processed_doc = {
            'url': url,
            'original_id': str(raw_doc['_id']),
            'original_title': raw_doc.get('title', ''),
            'scraped_at': raw_doc.get('timestamp'),
            'processed_at': datetime.utcnow().isoformat(),
            
            # Parsed data
            'title': parsed_data['title'],
            'headings': parsed_data['headings'],
            'paragraphs': parsed_data['paragraphs'],
            'lists': parsed_data['lists'],
            'links': parsed_data['links'],
            'images': parsed_data['images'],
            'metadata': parsed_data['metadata'],
            'main_content': parsed_data['main_content'],
            'clean_text': parsed_data['clean_text'],
            'word_count': parsed_data['word_count'],
            'language': parsed_data['language'],
            
            # Status
            'ready_for_rag': True if parsed_data['word_count'] > 50 else False,
        }
        
        return processed_doc
    
    def save_processed(self, doc):
        """
        Save processed document to MongoDB
        
        Args:
            doc: Processed document
        """
        try:
            self.processed_collection.insert_one(doc)
        except Exception as e:
            # Handle duplicates
            if 'duplicate key error' in str(e).lower():
                logger.warning(f"Document already processed: {doc['url']}")
            else:
                raise
    
    def get_stats(self):
        """Get processing statistics"""
        raw_total = self.raw_collection.count_documents({})
        raw_processed = self.raw_collection.count_documents({'processed': True})
        raw_unprocessed = self.raw_collection.count_documents({'processed': False})
        processed_total = self.processed_collection.count_documents({})
        
        stats = {
            'raw_documents': {
                'total': raw_total,
                'processed': raw_processed,
                'unprocessed': raw_unprocessed,
            },
            'processed_documents': {
                'total': processed_total,
                'ready_for_rag': self.processed_collection.count_documents({'ready_for_rag': True}),
            }
        }
        
        return stats
    
    def close(self):
        """Close database connection"""
        self.client.close()
        logger.info("Closed MongoDB connection")


def main():
    """Main processing function"""
    processor = HTMLProcessor()
    
    try:
        # Get initial stats
        stats = processor.get_stats()
        logger.info(f"Initial stats: {stats}")
        
        # Process all unprocessed documents
        processor.process_all_unprocessed()
        
        # Get final stats
        stats = processor.get_stats()
        logger.info(f"Final stats: {stats}")
        
    finally:
        processor.close()


if __name__ == '__main__':
    main()