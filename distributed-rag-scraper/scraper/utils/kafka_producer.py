"""
Kafka Producer for URL distribution
Sends URLs to Kafka topic for distributed processing

Why Kafka Producer?
- Decouples URL submission from scraping
- Provides durability (URLs saved to disk)
- Enables asynchronous processing
"""
from kafka import KafkaProducer
import json
import logging
from typing import List, Dict
import time

logger = logging.getLogger(__name__)


class URLProducer:
    """
    Kafka producer for sending URLs to scraping queue
    
    Why separate class?
    - Reusable across different parts of application
    - Manages connection lifecycle
    - Handles serialization automatically
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'scraping-tasks'
    ):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka server address
            topic: Topic name for scraping tasks
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        # Create Kafka producer
        # Why json serializer? Human-readable, easy to debug
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Why acks='all'? Ensures message is saved before confirming
            acks='all',
            # Why retries? Handle temporary network issues
            retries=3,
            # Why max_in_flight? Prevents overwhelming Kafka
            max_in_flight_requests_per_connection=5
        )
        
        logger.info(f"Kafka producer initialized: {bootstrap_servers}, topic: {topic}")
    
    def send_url(self, url: str, priority: int = 0, metadata: Dict = None) -> bool:
        """
        Send a single URL to Kafka
        
        Args:
            url: URL to scrape
            priority: Priority level (0=normal, 1=high, 2=urgent)
            metadata: Additional metadata (tags, source, etc.)
            
        Returns:
            bool: True if sent successfully
            
        Why priority?
        - Some URLs more important than others
        - Can process urgent URLs first
        """
        try:
            # Create message
            # Why include timestamp? Track how long URLs wait in queue
            message = {
                'url': url,
                'priority': priority,
                'timestamp': time.time(),
                'metadata': metadata or {}
            }
            
            # Send to Kafka
            # Why .get(timeout=10)? Wait for confirmation (blocking)
            future = self.producer.send(self.topic, value=message)
            result = future.get(timeout=10)
            
            logger.info(f"Sent URL to Kafka: {url} (partition: {result.partition}, offset: {result.offset})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send URL to Kafka: {url}, error: {e}")
            return False
    
    def send_urls_batch(self, urls: List[str], priority: int = 0) -> Dict:
        """
        Send multiple URLs in batch
        
        Args:
            urls: List of URLs to scrape
            priority: Priority level for all URLs
            
        Returns:
            dict: Statistics (sent, failed)
            
        Why batch?
        - More efficient than sending one-by-one
        - Better throughput
        - Reduces network overhead
        """
        stats = {
            'sent': 0,
            'failed': 0,
            'urls': []
        }
        
        logger.info(f"Sending batch of {len(urls)} URLs to Kafka")
        
        for url in urls:
            success = self.send_url(url, priority)
            if success:
                stats['sent'] += 1
                stats['urls'].append(url)
            else:
                stats['failed'] += 1
        
        # Flush to ensure all messages sent
        # Why flush? Force immediate sending instead of buffering
        self.producer.flush()
        
        logger.info(f"Batch complete: {stats['sent']} sent, {stats['failed']} failed")
        return stats
    
    def send_urls_from_file(self, filepath: str) -> Dict:
        """
        Read URLs from file and send to Kafka
        
        Args:
            filepath: Path to file with URLs (one per line)
            
        Returns:
            dict: Statistics
            
        Why from file?
        - Common use case: bulk URL submission
        - Easy to prepare large lists
        - Can be automated
        """
        try:
            with open(filepath, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            logger.info(f"Read {len(urls)} URLs from {filepath}")
            return self.send_urls_batch(urls)
            
        except Exception as e:
            logger.error(f"Failed to read URLs from file: {e}")
            return {'sent': 0, 'failed': 0, 'urls': []}
    
    def close(self):
        """
        Close Kafka producer connection
        
        Why close explicitly?
        - Ensures all buffered messages sent
        - Proper resource cleanup
        - Prevents data loss
        """
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")


def send_to_kafka(
    urls: List[str],
    bootstrap_servers: str = 'localhost:9092',
    topic: str = 'scraping-tasks'
) -> Dict:
    """
    Convenience function to send URLs to Kafka
    
    Args:
        urls: List of URLs
        bootstrap_servers: Kafka server
        topic: Topic name
        
    Returns:
        dict: Statistics
        
    Why convenience function?
    - Quick one-liner for simple cases
    - Handles connection management
    - Cleaner code
    """
    producer = URLProducer(bootstrap_servers, topic)
    try:
        return producer.send_urls_batch(urls)
    finally:
        producer.close()