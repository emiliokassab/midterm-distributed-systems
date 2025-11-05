"""
Kafka Consumer for processing scraping tasks
Pulls URLs from Kafka and triggers Ray workers

Why Kafka Consumer?
- Automatically distributes work across multiple consumers
- Provides fault tolerance (if consumer crashes, another picks up)
- Tracks message offsets (knows what's been processed)
"""
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge
import json
import logging
from typing import Callable
import time
from scraper.utils.distributed_scraper import DistributedScraper

logger = logging.getLogger(__name__)


class ScrapingConsumer:
    """
    Kafka consumer that processes scraping tasks
    
    Why consumer group?
    - Multiple consumers can work together
    - Kafka automatically balances load
    - If one consumer fails, others continue
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'scraping-tasks',
        group_id: str = 'scraper-group',
        num_ray_workers: int = 4
        
    ):
        """
        Initialize Kafka consumer
        
        Args:
            bootstrap_servers: Kafka server address
            topic: Topic to consume from
            group_id: Consumer group ID
            num_ray_workers: Number of Ray workers for scraping
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.alive = False  # ✅ added: flag to report consumer health (used for monitoring)

        try:
            # ✅ added: optional dynamic topic creation (if using admin client)
            from kafka.admin import KafkaAdminClient, NewTopic  # ✅ added
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)  # ✅ added
            existing_topics = admin_client.list_topics()  # ✅ added
            if topic not in existing_topics:  # ✅ added
                admin_client.create_topics([NewTopic(name=topic, num_partitions=3, replication_factor=1)])  # ✅ added
                logger.info(f"Created missing Kafka topic: {topic}")  # ✅ added
            admin_client.close()  # ✅ added
        except Exception as e:
            logger.warning(f"Kafka topic check/creation failed (may already exist): {e}")  # ✅ added
        
        # Create Kafka consumer
        # Why auto_offset_reset='earliest'? Process all messages from beginning
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            # Why json deserializer? Parse messages automatically
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # Why enable_auto_commit=False? Manual control over acknowledgment
            enable_auto_commit=False,
            # Why auto_offset_reset? Start from beginning if no offset saved
            auto_offset_reset='earliest',
            # Why max_poll_records? Don't overwhelm system
            max_poll_records=10
        )

         # ✅ added: fault tolerance — verify connection before starting
        try:
            self.consumer.poll(timeout_ms=1000)  # quick connection check
            logger.info("Kafka consumer connection verified")  # ✅ added
            self.alive = True  # ✅ added
        except Exception as e:
            logger.error(f"Kafka consumer connection failed: {e}")  # ✅ added
            self.alive = False  # ✅ added
        
        # Initialize Ray scraper
        self.scraper = DistributedScraper(num_workers=num_ray_workers)
        
        # Statistics
        self.stats = {
            'processed': 0,
            'errors': 0,
            'skipped': 0
        }
        
        logger.info(f"Kafka consumer initialized: {bootstrap_servers}, topic: {topic}, group: {group_id}")
    
    def start_consuming(self, max_messages: int = None):
        """
        Start consuming messages from Kafka
        
        Args:
            max_messages: Maximum messages to process (None = infinite)
            
        Why blocking loop?
        - Consumer runs continuously
        - Waits for new messages
        - Processes as they arrive
        """
        logger.info(f"Starting to consume from topic: {self.topic}")
        
        try:
            message_count = 0
            
            # Infinite loop - keeps running until stopped
            # Why infinite? Consumer should always be ready
            for message in self.consumer:
                try:
                    # Extract message data
                    task = message.value
                    url = task.get('url')
                    priority = task.get('priority', 0)
                    
                    logger.info(f"Received task: {url} (priority: {priority})")
                    
                    # Process the URL
                    result = self._process_url(url)
                    
                    # Commit offset only after successful processing
                    # Why manual commit? Ensures message not lost if processing fails
                    if result['success']:
                        self.consumer.commit()
                        self.stats['processed'] += 1
                        logger.info(f"✅ Processed and committed: {url}")
                    else:
                        self.stats['errors'] += 1
                        logger.error(f"❌ Failed to process: {url}")
                    
                    message_count += 1
                    
                    # Check if reached max messages
                    if max_messages and message_count >= max_messages:
                        logger.info(f"Reached max messages limit: {max_messages}")
                        break
                    
                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"Error processing message: {e}")
        
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        
        finally:
            self._print_stats()
    
    def _process_url(self, url: str) -> dict:
        """
        Process a single URL using Ray workers
        
        Args:
            url: URL to scrape
            
        Returns:
            dict: Processing result
            
        Why separate method?
        - Easy to test
        - Can be overridden
        - Clean separation of concerns
        """
        try:
            # Scrape using distributed scraper
            results = self.scraper.scrape_urls([url])
            
            if results and results[0]['status'] == 'success':
                return {'success': True, 'url': url}
            else:
                return {'success': False, 'url': url, 'error': 'Scraping failed'}
        
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return {'success': False, 'url': url, 'error': str(e)}
    
    def _print_stats(self):
        """Print consumer statistics"""
        logger.info("="*60)
        logger.info("Consumer Statistics:")
        logger.info(f"  Processed: {self.stats['processed']}")
        logger.info(f"  Errors: {self.stats['errors']}")
        logger.info(f"  Skipped: {self.stats['skipped']}")
        logger.info("="*60)
    
    def close(self):
        """
        Close consumer and cleanup
        
        Why cleanup?
        - Close database connections
        - Shutdown Ray workers
        - Commit final offsets
        """
        self.consumer.close()
        self.scraper.shutdown()
        logger.info("Consumer closed")


class SimpleConsumer:
    """
    Simplified Kafka consumer for testing
    
    Why simple version?
    - Quick testing without Ray
    - Easier to debug
    - Good for demos
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'scraping-tasks',
        group_id: str = 'test-group'
    ):
        """Initialize simple consumer"""
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        logger.info(f"Simple consumer initialized: {topic}")
    
    def consume_and_print(self, max_messages: int = 10):
        """
        Consume messages and print them
        
        Args:
            max_messages: How many messages to consume
            
        Why print?
        - Easy debugging
        - Verify messages in queue
        - Test Kafka connectivity
        """
        logger.info(f"Consuming up to {max_messages} messages...")
        
        count = 0
        for message in self.consumer:
            task = message.value
            print(f"\n{'='*60}")
            print(f"Message {count + 1}:")
            print(f"  URL: {task.get('url')}")
            print(f"  Priority: {task.get('priority')}")
            print(f"  Timestamp: {task.get('timestamp')}")
            print(f"  Metadata: {task.get('metadata')}")
            print(f"{'='*60}\n")
            
            count += 1
            if count >= max_messages:
                break
        
        logger.info(f"Consumed {count} messages")
        self.consumer.close()


def consume_from_kafka(
    bootstrap_servers: str = 'localhost:9092',
    topic: str = 'scraping-tasks',
    group_id: str = 'scraper-group',
    max_messages: int = None
):
    """
    Convenience function to start consuming
    
    Args:
        bootstrap_servers: Kafka server
        topic: Topic name
        group_id: Consumer group
        max_messages: Max messages to process
        
    Why convenience function?
    - Easy to use from command line
    - Quick testing
    - Minimal code
    """
    consumer = ScrapingConsumer(bootstrap_servers, topic, group_id)
    try:
        consumer.start_consuming(max_messages)
    finally:
        consumer.close()