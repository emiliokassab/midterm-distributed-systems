"""
Test RAG with Google Gemini (FREE!)
"""
import os
import logging
from dotenv import load_dotenv
from rag.vector_db.chroma_manager import ChromaManager
from rag.llm.gemini_client import GeminiRAGClient

# Load environment
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Test Gemini RAG"""
    
    logger.info("\n" + "🌟 "*20)
    logger.info("GEMINI RAG TEST (FREE!)")
    logger.info("🌟 "*20 + "\n")
    
    # Check API key
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        logger.error("❌ Gemini API key not found!")
        logger.info("\n📝 Quick Setup:")
        logger.info("1. Go to: https://aistudio.google.com/app/apikey")
        logger.info("2. Click 'Get API Key'")
        logger.info("3. Add to .env: GEMINI_API_KEY=your-key")
        return
    
    try:
        # Initialize
        chroma = ChromaManager()
        rag = GeminiRAGClient(api_key=api_key, chroma_manager=chroma)
        
        # Test questions
        questions = [
            "What are the most inspirational quotes about life?",
            "Can you find funny or humorous quotes?",
            "Tell me about quotes on success and achievement"
        ]
        
        for question in questions:
            logger.info(f"\n{'='*60}")
            logger.info(f"❓ Question: {question}")
            logger.info(f"{'='*60}")
            
            result = rag.query(question, n_results=3)
            
            logger.info(f"\n🤖 Gemini Answer:")
            logger.info(f"{result['answer']}")
            
            logger.info(f"\n📚 Sources Used:")
            for source in result['sources']:
                logger.info(f"  {source['source_number']}. {source['title']}")
                logger.info(f"     {source['url']}")
                logger.info(f"     Relevance: {source['relevance_score']:.3f}")
            
            logger.info("")
        
        logger.info("="*60)
        logger.info("🎉 Gemini RAG working perfectly!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")


if __name__ == '__main__':
    main()