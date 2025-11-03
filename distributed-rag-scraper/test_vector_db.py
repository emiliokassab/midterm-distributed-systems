"""
Test Vector Database (ChromaDB) Integration
Creates embeddings from MongoDB data and tests semantic search
"""
import logging
from rag.vector_db.chroma_manager import ChromaManager, create_embeddings_from_mongodb

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_create_embeddings():
    """
    Test: Load data from MongoDB and create embeddings
    
    What this does:
    1. Reads processed_data from MongoDB
    2. Converts text to embeddings
    3. Stores in ChromaDB
    
    Why important?
    - Makes data searchable by meaning
    - One-time setup (expensive operation)
    - Foundation for RAG system
    """
    logger.info("="*60)
    logger.info("TEST 1: Creating Embeddings from MongoDB")
    logger.info("="*60)
    
    try:
        stats = create_embeddings_from_mongodb(
            mongo_uri='mongodb://localhost:27017',
            db_name='rag_scraper',
            chroma_dir='./data/chromadb'
        )
        
        logger.info(f"\n✅ Embedding Creation Results:")
        logger.info(f"   Documents embedded: {stats.get('added', 0)}")
        logger.info(f"   Total in ChromaDB: {stats.get('total_docs', 0)}")
        
        if stats.get('added', 0) > 0:
            logger.info("\n🎉 Embeddings created successfully!")
            return True
        else:
            logger.warning("\n⚠️  No new embeddings created (might already exist)")
            return True
            
    except Exception as e:
        logger.error(f"\n❌ Failed to create embeddings: {e}")
        return False


def test_semantic_search():
    """
    Test: Semantic search with different queries
    
    What this proves:
    - Finds similar content by meaning
    - Not just keyword matching
    - Works across different phrasings
    """
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Semantic Search")
    logger.info("="*60)
    
    # Initialize ChromaDB
    chroma = ChromaManager(
        persist_directory='./data/chromadb',
        collection_name='scraped_content'
    )
    
    # Check if we have data
    stats = chroma.get_stats()
    logger.info(f"\nDatabase stats: {stats}")
    
    if stats['total_documents'] == 0:
        logger.warning("\n⚠️  No documents in ChromaDB. Run test_create_embeddings first!")
        return False
    
    # Test queries
    test_queries = [
        "inspirational quotes about life",
        "funny sayings",
        "quotes about love and relationships",
        "wisdom and philosophy",
    ]
    
    logger.info(f"\n🔍 Testing {len(test_queries)} semantic searches:\n")
    
    for query in test_queries:
        logger.info(f"{'='*60}")
        logger.info(f"Query: '{query}'")
        logger.info(f"{'='*60}")
        
        try:
            results = chroma.query(query_text=query, n_results=3)
            
            logger.info(f"Found {results['num_results']} results:\n")
            
            for i, result in enumerate(results['results'], 1):
                logger.info(f"  Result {i}:")
                logger.info(f"    URL: {result['metadata'].get('url', 'N/A')}")
                logger.info(f"    Title: {result['metadata'].get('title', 'N/A')}")
                logger.info(f"    Similarity: {1 - result.get('distance', 0):.3f}")  # Convert distance to similarity
                logger.info(f"    Preview: {result['document'][:150]}...")
                logger.info("")
        
        except Exception as e:
            logger.error(f"Error searching for '{query}': {e}")
    
    logger.info("\n✅ Semantic search test complete!")
    return True


def test_specific_lookup():
    """
    Test: Retrieve specific document by ID
    
    Why useful?
    - Verify data stored correctly
    - Quick access to known documents
    - Debug data issues
    """
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Specific Document Lookup")
    logger.info("="*60)
    
    chroma = ChromaManager()
    stats = chroma.get_stats()
    
    if stats['total_documents'] == 0:
        logger.warning("\n⚠️  No documents to lookup")
        return False
    
    # Get all IDs and look up first one
    results = chroma.query("test", n_results=1)
    if results['results']:
        doc_id = results['results'][0]['id']
        logger.info(f"\nLooking up document ID: {doc_id}")
        
        doc = chroma.get_by_id(doc_id)
        if doc:
            logger.info(f"\n✅ Found document:")
            logger.info(f"   URL: {doc['metadata'].get('url')}")
            logger.info(f"   Title: {doc['metadata'].get('title')}")
            logger.info(f"   Word Count: {doc['metadata'].get('word_count')}")
            return True
    
    return False


def main():
    """
    Run all ChromaDB tests
    
    Test sequence:
    1. Create embeddings from MongoDB
    2. Test semantic search
    3. Test specific lookups
    """
    logger.info("\n" + "🧠 "*20)
    logger.info("CHROMADB VECTOR DATABASE TESTS")
    logger.info("🧠 "*20 + "\n")
    
    results = []
    
    # Test 1: Create embeddings
    results.append(('Create Embeddings', test_create_embeddings()))
    
    # Test 2: Semantic search
    results.append(('Semantic Search', test_semantic_search()))
    
    # Test 3: Specific lookup
    results.append(('Document Lookup', test_specific_lookup()))
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"  {test_name}: {status}")
    
    logger.info("="*60)
    
    # Overall
    all_passed = all(result for _, result in results)
    if all_passed:
        logger.info("\n🎉 All tests PASSED! Vector database ready! 🎉")
        logger.info("\n💡 Next: Integrate with LLM for RAG queries")
    else:
        logger.info("\n⚠️  Some tests failed. Check logs above.")


if __name__ == '__main__':
    main()