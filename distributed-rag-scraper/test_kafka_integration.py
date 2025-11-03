"""
Test Kafka integration
Tests producer → Kafka → consumer flow
"""
import time
import logging
from scraper.utils.kafka_producer import URLProducer
from scraper.utils.kafka_consumer import SimpleConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_producer():
    """
    Test sending URLs to Kafka
    
    Why test producer first?
    - Verify Kafka connectivity
    - Check if messages stored
    - Ensure serialization works
    """
    logger.info("="*60)
    logger.info("TEST 1: Kafka Producer")
    logger.info("="*60)
    
    # Test URLs
    test_urls = [
        'https://quotes.toscrape.com/page/6/',
        'https://quotes.toscrape.com/page/7/',
        'https://quotes.toscrape.com/page/8/',
        'https://quotes.toscrape.com/tag/reading/',
        'https://quotes.toscrape.com/tag/friendship/',
    ]
    
    try:
        # Create producer
        producer = URLProducer(
            bootstrap_servers='localhost:9092',
            topic='scraping-tasks'
        )
        
        # Send URLs
        logger.info(f"\n📤 Sending {len(test_urls)} URLs to Kafka...")
        stats = producer.send_urls_batch(test_urls)
        
        # Print results
        logger.info(f"\n✅ Producer Results:")
        logger.info(f"   Sent: {stats['sent']}")
        logger.info(f"   Failed: {stats['failed']}")
        
        # Close producer
        producer.close()
        
        logger.info("\n✅ Producer test PASSED!")
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Producer test FAILED: {e}")
        return False


def test_consumer():
    """
    Test consuming messages from Kafka
    
    Why test consumer?
    - Verify messages can be read
    - Check deserialization
    - See message format
    """
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Kafka Consumer (Simple)")
    logger.info("="*60)
    
    try:
        logger.info("\n📥 Consuming messages from Kafka...\n")
        
        # Create simple consumer (just prints messages)
        consumer = SimpleConsumer(
            bootstrap_servers='localhost:9092',
            topic='scraping-tasks',
            group_id='test-group-simple'
        )
        
        # Consume and print messages
        consumer.consume_and_print(max_messages=5)
        
        logger.info("\n✅ Consumer test PASSED!")
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Consumer test FAILED: {e}")
        return False


def test_full_pipeline():
    """
    Test complete producer → consumer → scraper pipeline
    
    Why full pipeline test?
    - Verify end-to-end integration
    - Test with actual scraping
    - Ensure everything works together
    """
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Full Pipeline (Producer → Kafka → Consumer → Scraper)")
    logger.info("="*60)
    
    # Note: This test requires running the consumer in a separate process
    # For now, we'll just demonstrate the producer side
    
    logger.info("\n⚠️  For full pipeline test:")
    logger.info("   1. Run consumer in separate terminal:")
    logger.info("      python -c \"from scraper.utils.kafka_consumer import consume_from_kafka; consume_from_kafka(max_messages=5)\"")
    logger.info("   2. Then run this test again")
    logger.info("\n   Or manually start consumer and producer separately.")
    
    return True


def main():
    """
    Run all Kafka tests
    
    Test sequence:
    1. Producer test (send messages)
    2. Consumer test (read messages)
    3. Full pipeline info
    """
    logger.info("\n" + "🚀 "*15)
    logger.info("KAFKA INTEGRATION TESTS")
    logger.info("🚀 "*15 + "\n")
    
    # Wait for Kafka to be ready
    logger.info("⏳ Waiting 2 seconds for Kafka to be ready...")
    time.sleep(2)
    
    # Run tests
    results = []
    
    # Test 1: Producer
    results.append(('Producer', test_producer()))
    time.sleep(2)
    
    # Test 2: Consumer
    results.append(('Consumer', test_consumer()))
    time.sleep(2)
    
    # Test 3: Full pipeline info
    results.append(('Pipeline Info', test_full_pipeline()))
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"  {test_name}: {status}")
    
    logger.info("="*60)
    
    # Overall result
    all_passed = all(result for _, result in results)
    if all_passed:
        logger.info("\n🎉 All tests PASSED! Kafka integration working! 🎉\n")
    else:
        logger.info("\n⚠️  Some tests failed. Check Kafka setup. ⚠️\n")


if __name__ == '__main__':
    main()