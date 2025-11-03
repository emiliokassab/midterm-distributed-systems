"""
Test distributed scraping with Ray
Compare sequential vs parallel scraping
"""
import time
from scraper.utils.distributed_scraper import scrape_distributed
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """
    Test distributed scraping
    
    Why test URLs from quotes.toscrape.com?
    - Safe, legal to scrape
    - Fast response times
    - Consistent structure
    """
    
    # Test URLs - different pages from quotes site
    test_urls = [
        'https://quotes.toscrape.com/page/1/',
        'https://quotes.toscrape.com/page/2/',
        'https://quotes.toscrape.com/page/3/',
        'https://quotes.toscrape.com/page/4/',
        'https://quotes.toscrape.com/page/5/',
        'https://quotes.toscrape.com/tag/love/',
        'https://quotes.toscrape.com/tag/inspirational/',
        'https://quotes.toscrape.com/tag/life/',
        'https://quotes.toscrape.com/tag/humor/',
        'https://quotes.toscrape.com/tag/books/',
    ]
    
    logger.info(f"Testing distributed scraping with {len(test_urls)} URLs")
    logger.info("=" * 60)
    
    # Test with different number of workers
    for num_workers in [1, 2, 4]:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing with {num_workers} workers")
        logger.info(f"{'='*60}")
        
        start_time = time.time()
        
        # Run distributed scraping
        stats = scrape_distributed(
            urls=test_urls,
            num_workers=num_workers,
            mongo_uri='mongodb://localhost:27017',
            db_name='rag_scraper'
        )
        
        elapsed_time = time.time() - start_time
        
        # Print results
        logger.info(f"\n📊 Results with {num_workers} workers:")
        logger.info(f"   ⏱️  Time taken: {elapsed_time:.2f} seconds")
        logger.info(f"   ✅ Successfully scraped: {stats['total_scraped']}")
        logger.info(f"   ⚠️  Duplicates skipped: {stats['total_duplicates']}")
        logger.info(f"   ❌ Errors: {stats['total_errors']}")
        logger.info(f"   📈 Speed: {len(test_urls)/elapsed_time:.2f} URLs/second")
        
        # Show per-worker stats
        logger.info(f"\n   Worker breakdown:")
        for worker_stat in stats['worker_stats']:
            logger.info(f"      Worker {worker_stat['worker_id']}: "
                       f"scraped={worker_stat['scraped']}, "
                       f"errors={worker_stat['errors']}, "
                       f"duplicates={worker_stat['duplicates']}")
        
        time.sleep(2)  # Brief pause between tests
    
    logger.info(f"\n{'='*60}")
    logger.info("✅ Testing complete!")
    logger.info(f"{'='*60}")
    
    logger.info("\n💡 Key Observations:")
    logger.info("   - More workers = faster scraping (up to a point)")
    logger.info("   - 4 workers should be ~4x faster than 1 worker")
    logger.info("   - Diminishing returns after CPU core count")


if __name__ == '__main__':
    main()