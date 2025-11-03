"""
Run FastAPI Service
Starts the API server with proper configuration
"""
import os
import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Set environment variables if .env doesn't have them
os.environ.setdefault("API_TOKEN", "demo-token-123")
os.environ.setdefault("MONGODB_HOST", "localhost")
os.environ.setdefault("MONGODB_PORT", "27017")
os.environ.setdefault("MONGODB_DATABASE", "rag_scraper")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("API_PORT", "8000")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Start the FastAPI server"""
    logger.info("=" * 60)
    logger.info("STARTING FASTAPI SERVICE")
    logger.info("=" * 60)
    
    # Check dependencies
    try:
        import fastapi
        import uvicorn
        import redis
        import pymongo
        logger.info("✅ All dependencies installed")
    except ImportError as e:
        logger.error(f"❌ Missing dependency: {e}")
        logger.info("Install with: pip install fastapi uvicorn redis pymongo")
        return
    
    # Start server
    logger.info(f"Starting API server on http://localhost:8000")
    logger.info(f"API Documentation: http://localhost:8000/docs")
    logger.info(f"Authentication Token: demo-token-123")
    logger.info("Press Ctrl+C to stop")
    
    import uvicorn
    uvicorn.run(
        "api_service:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    main()