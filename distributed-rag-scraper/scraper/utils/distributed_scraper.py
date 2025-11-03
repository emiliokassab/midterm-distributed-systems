"""
Distributed Web Scraper using Ray
Enables parallel scraping across multiple workers
"""
import ray
import requests
from bs4 import BeautifulSoup
import hashlib
from datetime import datetime
from typing import List, Dict
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@ray.remote
class ScraperWorker:
    """
    Ray Actor for distributed scraping
    
    Why Actor? 
    - Each worker maintains its own state (database connection)
    - Can handle multiple tasks sequentially
    - More efficient than creating new connections per task
    """
    
    def __init__(self, worker_id: int, mongo_uri: str, db_name: str):
        """
        Initialize a scraper worker
        
        Args:
            worker_id: Unique worker identifier
            mongo_uri: MongoDB connection string
            db_name: Database name
        """
        self.worker_id = worker_id
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        
        # Each worker gets its own database connection
        # Why? Avoid connection conflicts between workers
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db['raw_data']
        
        # Create index for duplicate detection
        self.collection.create_index('url', unique=True)
        
        # Statistics tracking
        self.stats = {
            'scraped': 0,
            'errors': 0,
            'duplicates': 0
        }
        
        logger.info(f"Worker {worker_id} initialized")
    
    def scrape_url(self, url: str) -> Dict:
        """
        Scrape a single URL
        
        Args:
            url: URL to scrape
            
        Returns:
            dict: Scraping result with status
        """
        try:
            # Make HTTP request
            # Why timeout? Don't wait forever for slow sites
            response = requests.get(
                url,
                timeout=30,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            )
            
            # Check if successful
            if response.status_code != 200:
                self.stats['errors'] += 1
                return {
                    'url': url,
                    'status': 'error',
                    'message': f'HTTP {response.status_code}',
                    'worker_id': self.worker_id
                }
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Extract data
            data = {
                'id': hashlib.md5(url.encode()).hexdigest(),
                'url': url,
                'title': soup.find('title').get_text().strip() if soup.find('title') else '',
                'html': response.text,
                'status_code': response.status_code,
                'headers': {k: v for k, v in response.headers.items()},
                'timestamp': datetime.utcnow().isoformat(),
                'processed': False,
                'worker_id': self.worker_id,
            }
            
            # Save to MongoDB
            try:
                self.collection.insert_one(data)
                self.stats['scraped'] += 1
                
                return {
                    'url': url,
                    'status': 'success',
                    'worker_id': self.worker_id,
                    'title': data['title']
                }
                
            except DuplicateKeyError:
                self.stats['duplicates'] += 1
                return {
                    'url': url,
                    'status': 'duplicate',
                    'worker_id': self.worker_id
                }
        
        except requests.RequestException as e:
            self.stats['errors'] += 1
            return {
                'url': url,
                'status': 'error',
                'message': str(e),
                'worker_id': self.worker_id
            }
        
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Worker {self.worker_id} error on {url}: {e}")
            return {
                'url': url,
                'status': 'error',
                'message': str(e),
                'worker_id': self.worker_id
            }
    
    def get_stats(self) -> Dict:
        """
        Get worker statistics
        
        Returns:
            dict: Worker stats
        """
        return {
            'worker_id': self.worker_id,
            **self.stats
        }
    
    def shutdown(self):
        """Close database connection"""
        self.client.close()
        logger.info(f"Worker {self.worker_id} shutdown")


class DistributedScraper:
    """
    Distributed scraper coordinator
    
    Why a coordinator?
    - Manages multiple workers
    - Distributes URLs to workers
    - Collects results
    - Provides overall statistics
    """
    
    def __init__(
        self, 
        num_workers: int = 4,
        mongo_uri: str = 'mongodb://localhost:27017',
        db_name: str = 'rag_scraper'
    ):
        """
        Initialize distributed scraper
        
        Args:
            num_workers: Number of parallel workers
            mongo_uri: MongoDB connection string
            db_name: Database name
        """
        self.num_workers = num_workers
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        
        # Initialize Ray
        # Why Ray? Handles all the complexity of distributed computing
        if not ray.is_initialized():
            ray.init(
                num_cpus=num_workers,
                ignore_reinit_error=True,
                logging_level=logging.INFO
            )
        
        # Create worker pool
        # Why actors? Long-lived workers that maintain state
        self.workers = [
            ScraperWorker.remote(i, mongo_uri, db_name)
            for i in range(num_workers)
        ]
        
        logger.info(f"Initialized {num_workers} scraper workers")
    
    def scrape_urls(self, urls: List[str]) -> List[Dict]:
        """
        Scrape multiple URLs in parallel
        
        Args:
            urls: List of URLs to scrape
            
        Returns:
            list: Scraping results
        """
        logger.info(f"Starting to scrape {len(urls)} URLs with {self.num_workers} workers")
        
        # Distribute URLs to workers
        # Why round-robin? Ensures even distribution of work
        futures = []
        for i, url in enumerate(urls):
            worker_idx = i % self.num_workers  # Round-robin assignment
            future = self.workers[worker_idx].scrape_url.remote(url)
            futures.append(future)
        
        # Wait for all tasks to complete
        # Why ray.get? Blocks until all results are ready
        results = ray.get(futures)
        
        logger.info(f"Completed scraping {len(results)} URLs")
        return results
    
    def get_all_stats(self) -> List[Dict]:
        """
        Get statistics from all workers
        
        Returns:
            list: Stats from each worker
        """
        # Collect stats from all workers in parallel
        futures = [worker.get_stats.remote() for worker in self.workers]
        stats = ray.get(futures)
        return stats
    
    def get_summary_stats(self) -> Dict:
        """
        Get aggregated statistics
        
        Returns:
            dict: Summary statistics
        """
        all_stats = self.get_all_stats()
        
        summary = {
            'total_scraped': sum(s['scraped'] for s in all_stats),
            'total_errors': sum(s['errors'] for s in all_stats),
            'total_duplicates': sum(s['duplicates'] for s in all_stats),
            'num_workers': self.num_workers,
            'worker_stats': all_stats
        }
        
        return summary
    
    def shutdown(self):
        """Shutdown all workers and Ray"""
        # Shutdown all workers
        futures = [worker.shutdown.remote() for worker in self.workers]
        ray.get(futures)
        
        # Shutdown Ray
        ray.shutdown()
        logger.info("Distributed scraper shutdown complete")


def scrape_distributed(
    urls: List[str],
    num_workers: int = 4,
    mongo_uri: str = 'mongodb://localhost:27017',
    db_name: str = 'rag_scraper'
) -> Dict:
    """
    Convenience function for distributed scraping
    
    Args:
        urls: List of URLs to scrape
        num_workers: Number of parallel workers
        mongo_uri: MongoDB connection string
        db_name: Database name
        
    Returns:
        dict: Summary statistics
    """
    scraper = DistributedScraper(num_workers, mongo_uri, db_name)
    
    try:
        # Scrape URLs
        results = scraper.scrape_urls(urls)
        
        # Get statistics
        stats = scraper.get_summary_stats()
        stats['results'] = results
        
        return stats
        
    finally:
        scraper.shutdown()