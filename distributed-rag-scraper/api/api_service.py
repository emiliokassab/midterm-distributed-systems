"""
FastAPI Service for RAG-Based Web Scraper
Provides REST API endpoints for data access and querying
"""
from fastapi import FastAPI, HTTPException, Depends, Query, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import os
import logging
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
import redis
import hashlib
import time
import sys
from pathlib import Path

# Add parent directory to path to import from rag folder
sys.path.append(str(Path(__file__).parent.parent))

# Now import your modules
from rag.vector_db.chroma_manager import ChromaManager
from rag.llm.gemini_client import GeminiRAGClient
# Import your existing modules
from rag.vector_db.chroma_manager import ChromaManager
from rag.llm.gemini_client import GeminiRAGClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Distributed RAG Scraper API",
    description="API for accessing scraped data and RAG-enhanced content",
    version="1.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()

# Database connections
mongodb_client = MongoClient(
    host=os.getenv("MONGODB_HOST", "localhost"),
    port=int(os.getenv("MONGODB_PORT", 27017))
)
db = mongodb_client[os.getenv("MONGODB_DATABASE", "rag_scraper")]
raw_collection = db["raw_scraped_data"]
processed_collection = db["processed_data"]

# Redis for caching and rate limiting
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

# Initialize RAG components
chroma_manager = ChromaManager()
rag_client = GeminiRAGClient(chroma_manager=chroma_manager)

# Request/Response Models
class ScrapeRequest(BaseModel):
    url: str = Field(..., description="URL to scrape")
    depth: int = Field(1, description="Crawl depth", ge=1, le=3)

class QueryRequest(BaseModel):
    query: str = Field(..., description="Search query")
    n_results: int = Field(3, description="Number of results", ge=1, le=10)
    use_rag: bool = Field(True, description="Use RAG enhancement")

class RAGResponse(BaseModel):
    answer: str
    sources: List[Dict[str, Any]]
    question: str
    model: Optional[str] = None

# Authentication
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Simple token verification - expand for production"""
    token = credentials.credentials
    expected_token = os.getenv("API_TOKEN", "demo-token-123")
    
    if token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid authentication token"
        )
    return token

# Rate limiting
def rate_limit(key: str, max_requests: int = 10, window: int = 60):
    """Rate limiting using Redis"""
    current = redis_client.get(key)
    
    if current is None:
        redis_client.setex(key, window, 1)
        return True
    elif int(current) < max_requests:
        redis_client.incr(key)
        return True
    else:
        return False

# Cache decorator
def cache_response(expire: int = 300):
    """Cache API responses in Redis"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key = f"cache:{func.__name__}:{str(args)}:{str(kwargs)}"
            cache_key = hashlib.md5(cache_key.encode()).hexdigest()
            
            # Check cache
            cached = redis_client.get(cache_key)
            if cached:
                logger.info(f"Cache hit for {func.__name__}")
                return JSONResponse(content=eval(cached))
            
            # Call function
            result = await func(*args, **kwargs)
            
            # Store in cache
            redis_client.setex(cache_key, expire, str(result))
            
            return result
        return wrapper
    return decorator

# API Endpoints

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "Distributed RAG Scraper API",
        "version": "1.0.0",
        "endpoints": [
            "/docs",
            "/health",
            "/api/v1/raw-data",
            "/api/v1/processed-data",
            "/api/v1/search",
            "/api/v1/rag/query",
            "/api/v1/scrape"
        ]
    }

@app.get("/health")
async def health_check():
    """System health check"""
    try:
        # Check MongoDB
        mongodb_client.admin.command('ping')
        mongo_status = "healthy"
    except:
        mongo_status = "unhealthy"
    
    try:
        # Check Redis
        redis_client.ping()
        redis_status = "healthy"
    except:
        redis_status = "unhealthy"
    
    # Check vector DB
    doc_count = chroma_manager.get_collection_size()
    
    return {
        "status": "operational",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "mongodb": mongo_status,
            "redis": redis_status,
            "chromadb": f"{doc_count} documents"
        }
    }

@app.get("/api/v1/raw-data")
async def get_raw_data(
    limit: int = Query(10, ge=1, le=100),
    skip: int = Query(0, ge=0),
    url: Optional[str] = None,
    token: str = Depends(verify_token)
):
    """Fetch raw scraped data"""
    
    # Rate limiting
    if not rate_limit(f"rate:{token}", max_requests=20):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded"
        )
    
    # Build query
    query = {}
    if url:
        query["url"] = {"$regex": url, "$options": "i"}
    
    # Fetch data
    cursor = raw_collection.find(query).skip(skip).limit(limit)
    data = []
    
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        data.append(doc)
    
    total = raw_collection.count_documents(query)
    
    return {
        "data": data,
        "pagination": {
            "total": total,
            "limit": limit,
            "skip": skip,
            "has_more": (skip + limit) < total
        }
    }

@app.get("/api/v1/processed-data")
async def get_processed_data(
    limit: int = Query(10, ge=1, le=100),
    skip: int = Query(0, ge=0),
    url: Optional[str] = None,
    token: str = Depends(verify_token)
):
    """Fetch processed/cleaned data"""
    
    # Rate limiting
    if not rate_limit(f"rate:{token}", max_requests=20):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded"
        )
    
    # Build query
    query = {}
    if url:
        query["url"] = {"$regex": url, "$options": "i"}
    
    # Fetch data
    cursor = processed_collection.find(query).skip(skip).limit(limit)
    data = []
    
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        data.append(doc)
    
    total = processed_collection.count_documents(query)
    
    return {
        "data": data,
        "pagination": {
            "total": total,
            "limit": limit,
            "skip": skip,
            "has_more": (skip + limit) < total
        }
    }

@app.post("/api/v1/search")
async def search_data(
    request: QueryRequest,
    token: str = Depends(verify_token)
):
    """Search indexed data using semantic search"""
    
    # Rate limiting
    if not rate_limit(f"rate:{token}", max_requests=15):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded"
        )
    
    try:
        # Semantic search using ChromaDB
        results = chroma_manager.query(
            query_text=request.query,
            n_results=request.n_results
        )
        
        # Format response
        formatted_results = []
        for i, (doc, metadata, distance) in enumerate(zip(
            results["results"][0]["documents"],
            results["results"][0]["metadatas"],
            results["results"][0]["distances"]
        )):
            formatted_results.append({
                "rank": i + 1,
                "content": doc[:500] + "..." if len(doc) > 500 else doc,
                "metadata": metadata,
                "relevance_score": 1 - distance,
                "url": metadata.get("url", "Unknown")
            })
        
        return {
            "query": request.query,
            "results": formatted_results,
            "total_results": len(formatted_results)
        }
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )

@app.post("/api/v1/rag/query", response_model=RAGResponse)
async def rag_query(
    request: QueryRequest,
    token: str = Depends(verify_token)
):
    """Query data with RAG enhancement using Gemini"""
    
    # Rate limiting (stricter for RAG due to LLM costs)
    if not rate_limit(f"rate:rag:{token}", max_requests=10, window=60):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="RAG query rate limit exceeded"
        )
    
    try:
        if request.use_rag:
            # Full RAG query with Gemini
            result = rag_client.query(
                question=request.query,
                n_results=request.n_results,
                temperature=0.7
            )
        else:
            # Simple search without LLM
            search_results = chroma_manager.query(
                query_text=request.query,
                n_results=request.n_results
            )
            
            # Format as RAG response
            sources = []
            for i, (doc, metadata) in enumerate(zip(
                search_results["results"][0]["documents"],
                search_results["results"][0]["metadatas"]
            )):
                sources.append({
                    "source_number": i + 1,
                    "url": metadata.get("url", "Unknown"),
                    "title": metadata.get("title", "Untitled"),
                    "preview": doc[:200] + "..."
                })
            
            result = {
                "answer": "Retrieved documents without AI enhancement.",
                "sources": sources,
                "question": request.query,
                "model": None
            }
        
        return RAGResponse(**result)
        
    except Exception as e:
        logger.error(f"RAG query error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"RAG query failed: {str(e)}"
        )

@app.post("/api/v1/scrape")
async def scrape_url(
    request: ScrapeRequest,
    token: str = Depends(verify_token)
):
    """Add URL to scraping queue"""
    
    # Rate limiting
    if not rate_limit(f"rate:scrape:{token}", max_requests=5, window=60):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Scraping rate limit exceeded"
        )
    
    try:
        # Here you would send to Kafka queue
        # For now, return acknowledgment
        task_id = hashlib.md5(f"{request.url}{time.time()}".encode()).hexdigest()
        
        # Store task in Redis
        redis_client.setex(
            f"scrape:task:{task_id}",
            3600,
            str({"url": request.url, "depth": request.depth, "status": "queued"})
        )
        
        return {
            "task_id": task_id,
            "url": request.url,
            "depth": request.depth,
            "status": "queued",
            "message": "URL added to scraping queue"
        }
        
    except Exception as e:
        logger.error(f"Scraping submission error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to queue scraping task: {str(e)}"
        )

@app.get("/api/v1/stats")
async def get_statistics(token: str = Depends(verify_token)):
    """Get system statistics"""
    
    stats = {
        "raw_documents": raw_collection.count_documents({}),
        "processed_documents": processed_collection.count_documents({}),
        "vector_documents": chroma_manager.get_collection_size(),
        "unique_urls": len(raw_collection.distinct("url")),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return stats

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run(
        "api_service:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    )