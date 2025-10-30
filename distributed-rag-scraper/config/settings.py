"""
Configuration settings for the Distributed RAG-Based Web Scraper Framework
Windows-compatible version
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project Root - Works on Windows automatically
BASE_DIR = Path(__file__).resolve().parent.parent

# =============================================================================
# SCRAPING CONFIGURATION
# =============================================================================
SCRAPER_CONFIG = {
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'concurrent_requests': 16,
    'download_delay': 1,
    'autothrottle_enabled': True,
    'autothrottle_target_concurrency': 2.0,
    'retry_times': 3,
    'timeout': 30,
}

# Target URLs (example)
SEED_URLS = [
    'https://example.com',
    # Add your target URLs
]

# =============================================================================
# DISTRIBUTED COMPUTING
# =============================================================================
RAY_CONFIG = {
    'num_cpus': os.cpu_count() or 4,
    'num_gpus': 0,
    'dashboard_host': '0.0.0.0',
    'dashboard_port': 8265,
}

DASK_CONFIG = {
    'n_workers': 4,
    'threads_per_worker': 2,
    'dashboard_address': ':8787',
}

# =============================================================================
# MESSAGE QUEUE
# =============================================================================
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'scraping_topic': 'scraping-tasks',
    'processing_topic': 'processing-tasks',
    'group_id': 'rag-scraper-group',
}

RABBITMQ_CONFIG = {
    'host': os.getenv('RABBITMQ_HOST', 'localhost'),
    'port': int(os.getenv('RABBITMQ_PORT', 5672)),
    'username': os.getenv('RABBITMQ_USER', 'guest'),
    'password': os.getenv('RABBITMQ_PASS', 'guest'),
    'queue_name': 'scraping_queue',
}

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================
MONGODB_CONFIG = {
    'host': os.getenv('MONGODB_HOST', 'localhost'),
    'port': int(os.getenv('MONGODB_PORT', 27017)),
    'database': os.getenv('MONGODB_DATABASE', 'rag_scraper'),
    'username': os.getenv('MONGODB_USER'),
    'password': os.getenv('MONGODB_PASS'),
    'raw_collection': 'raw_data',
    'processed_collection': 'processed_data',
}

POSTGRESQL_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'rag_scraper'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASS', 'postgres'),
}

# =============================================================================
# VECTOR DATABASE
# =============================================================================
VECTOR_DB_TYPE = os.getenv('VECTOR_DB_TYPE', 'chromadb')

CHROMADB_CONFIG = {
    'persist_directory': str(BASE_DIR / 'data' / 'chromadb'),
    'collection_name': 'scraped_embeddings',
}

PINECONE_CONFIG = {
    'api_key': os.getenv('PINECONE_API_KEY'),
    'environment': os.getenv('PINECONE_ENV', 'us-west1-gcp'),
    'index_name': 'rag-scraper-index',
}

# =============================================================================
# LLM CONFIGURATION
# =============================================================================
LLM_PROVIDER = os.getenv('LLM_PROVIDER', 'openai')

OPENAI_CONFIG = {
    'api_key': os.getenv('OPENAI_API_KEY'),
    'model': os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo'),
    'embedding_model': os.getenv('OPENAI_EMBEDDING_MODEL', 'text-embedding-ada-002'),
    'max_tokens': 1000,
    'temperature': 0.7,
}

HUGGINGFACE_CONFIG = {
    'model_name': 'sentence-transformers/all-MiniLM-L6-v2',
    'device': 'cpu',
}

# =============================================================================
# RAG CONFIGURATION
# =============================================================================
RAG_CONFIG = {
    'chunk_size': 1000,
    'chunk_overlap': 200,
    'top_k': 5,
    'similarity_threshold': 0.7,
}

# =============================================================================
# API CONFIGURATION
# =============================================================================
API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', 8000)),
    'reload': os.getenv('API_RELOAD', 'true').lower() == 'true',
    'workers': int(os.getenv('API_WORKERS', 4)),
}

# Security
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Rate Limiting
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60

# =============================================================================
# MONITORING
# =============================================================================
PROMETHEUS_CONFIG = {
    'port': int(os.getenv('PROMETHEUS_PORT', 9090)),
    'path': '/metrics',
}

# =============================================================================
# LOGGING
# =============================================================================
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'default',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': str(BASE_DIR / 'logs' / 'app.log'),
            'maxBytes': 10485760,
            'backupCount': 5,
        },
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file'],
    },
}

# =============================================================================
# DATA PATHS (Windows compatible)
# =============================================================================
DATA_PATHS = {
    'raw': BASE_DIR / 'data' / 'raw',
    'processed': BASE_DIR / 'data' / 'processed',
    'indexed': BASE_DIR / 'data' / 'indexed',
    'logs': BASE_DIR / 'logs',
}

# Ensure directories exist
for path in DATA_PATHS.values():
    path.mkdir(parents=True, exist_ok=True)