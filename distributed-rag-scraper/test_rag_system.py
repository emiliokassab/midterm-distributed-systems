"""
Test RAG System (Retrieval-Augmented Generation)
Tests vector database + LLM integration
"""
import os
import logging
from rag.vector_db.chroma_manager import ChromaManager
from rag.llm.openai_client import RAGClient, SimpleRAG
from dotenv import load_dotenv

# Load .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_simple_rag():
    """
    Test RAG without OpenAI (retrieval only)
    
    Why test without API?
    - Verify retrieval works
    - No API costs
    - Good for development
    """
    logger.info("="*60)
    logger.info("TEST 1: Simple RAG (Retrieval Only)")
    logger.info("="*60)
    
    try:
        # Initialize ChromaDB
        chroma = ChromaManager()
        
        # Create simple RAG
        rag = SimpleRAG(chroma)
        
        # Test queries
        questions = [
            "What are inspirational quotes?",
            "Tell me about humor",
            "Quotes about life"
        ]
        
        for question in questions:
            logger.info(f"\n{'='*60}")
            logger.info(f"Question: {question}")
            logger.info(f"{'='*60}")
            
            result = rag.query(question, n_results=2)
            
            logger.info(f"\nRetrieved {len(result['retrieved_documents'])} documents:")
            
            for i, doc in enumerate(result['retrieved_documents'], 1):
                logger.info(f"\n  Document {i}:")
                logger.info(f"    URL: {doc['metadata'].get('url')}")
                logger.info(f"    Title: {doc['metadata'].get('title')}")
                logger.info(f"    Preview: {doc['document'][:150]}...")
        
        logger.info("\n✅ Simple RAG test complete!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Simple RAG test failed: {e}")
        return False


def test_full_rag():
    """
    Test RAG with OpenAI (if API key available)
    
    Why?
    - Test complete RAG pipeline
    - Verify LLM integration
    - See intelligent answers
    """
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Full RAG with OpenAI")
    logger.info("="*60)
    
    # Check for API key
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key:
        logger.warning("\n⚠️  OpenAI API key not found!")
        logger.info("Set it with: export OPENAI_API_KEY='your-key-here'")
        logger.info("Or add to .env file")
        logger.info("\n⏩ Skipping full RAG test")
        return None
    
    try:
        # Initialize ChromaDB
        chroma = ChromaManager()
        
        # Create RAG client
        rag = RAGClient(
            api_key=api_key,
            model="gpt-3.5-turbo",
            chroma_manager=chroma
        )
        
        # Test questions
        questions = [
            "What are the most inspirational quotes about life?",
            "Can you find funny or humorous quotes?",
        ]
        
        for question in questions:
            logger.info(f"\n{'='*60}")
            logger.info(f"Question: {question}")
            logger.info(f"{'='*60}")
            
            result = rag.query(question, n_results=3)
            
            logger.info(f"\n🤖 AI Answer:")
            logger.info(f"{result['answer']}")
            
            logger.info(f"\n📚 Sources Used:")
            for source in result['sources']:
                logger.info(f"  {source['source_number']}. {source['title']}")
                logger.info(f"     {source['url']}")
                logger.info(f"     Relevance: {source['relevance_score']:.3f}")
            
            if result.get('tokens_used'):
                logger.info(f"\n💰 Tokens used: {result['tokens_used']}")
        
        logger.info("\n✅ Full RAG test complete!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Full RAG test failed: {e}")
        return False


def main():
    """
    Run all RAG tests
    """
    logger.info("\n" + "🤖 "*20)
    logger.info("RAG SYSTEM TESTS")
    logger.info("🤖 "*20 + "\n")
    
    results = []
    
    # Test 1: Simple RAG (always works)
    results.append(('Simple RAG (Retrieval)', test_simple_rag()))
    
    # Test 2: Full RAG (needs API key)
    full_rag_result = test_full_rag()
    if full_rag_result is not None:
        results.append(('Full RAG (with AI)', full_rag_result))
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"  {test_name}: {status}")
    
    logger.info("="*60)
    
    # Instructions
    if len(results) == 1:
        logger.info("\n💡 To test with OpenAI:")
        logger.info("1. Get API key from https://platform.openai.com/api-keys")
        logger.info("2. Add to .env file: OPENAI_API_KEY=your-key-here")
        logger.info("3. Run this test again")
    else:
        logger.info("\n🎉 RAG system fully operational!")
        logger.info("Ready for API integration!")


if __name__ == '__main__':
    main()