"""
ChromaDB Vector Database Manager
Stores and retrieves text embeddings for semantic search

What are embeddings?
- Numbers that represent meaning of text
- Similar text = similar numbers
- Example: "love" and "affection" have similar embeddings

Why ChromaDB?
- Fast semantic search
- Built-in embedding generation
- Simple Python API
- No external services needed
"""
import chromadb
from chromadb.config import Settings
from typing import List, Dict, Optional
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ChromaManager:
    """
    Manages ChromaDB vector database operations
    
    Key Concepts:
    - Collection: Like a table in SQL, stores related documents
    - Embedding: Vector representation of text (list of numbers)
    - Metadata: Additional info (URL, title, date, etc.)
    - Distance: How similar two embeddings are
    """
    
    def __init__(
        self,
        persist_directory: str = './data/chromadb',
        collection_name: str = 'scraped_content'
    ):
        """
        Initialize ChromaDB client
        
        Args:
            persist_directory: Where to save database (survives restarts)
            collection_name: Name of collection to use
            
        Why persist?
        - Embeddings expensive to generate
        - Save once, query many times
        - No need to recompute
        """
        # Create directory if doesn't exist
        Path(persist_directory).mkdir(parents=True, exist_ok=True)
        
        # Initialize ChromaDB client
        # Why persistent? Database survives program restarts
        self.client = chromadb.PersistentClient(
            path=persist_directory,
            settings=Settings(
                anonymized_telemetry=False,  # Disable analytics
                allow_reset=True
            )
        )
        
        # Get or create collection
        # Why get_or_create? Safe for multiple runs
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "Scraped web content with embeddings"}
        )
        
        self.collection_name = collection_name
        self.persist_directory = persist_directory
        
        logger.info(f"ChromaDB initialized: {persist_directory}/{collection_name}")
        logger.info(f"Current document count: {self.collection.count()}")
    
    def add_documents(
        self,
        documents: List[str],
        metadatas: List[Dict],
        ids: List[str]
    ) -> Dict:
        """
        Add documents to vector database
        
        Args:
            documents: List of text content
            metadatas: List of metadata dicts (url, title, etc.)
            ids: Unique IDs for each document
            
        Returns:
            dict: Statistics
            
        What happens internally?
        1. Text → Embedding (automatic)
        2. Store embedding + text + metadata
        3. Create index for fast search
        
        Why metadata?
        - Can filter by URL, date, category
        - Provides context for results
        - Enables hybrid search (semantic + filters)
        """
        try:
            # Add to ChromaDB (embeddings generated automatically!)
            # Why automatic? ChromaDB uses sentence-transformers by default
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            
            logger.info(f"Added {len(documents)} documents to ChromaDB")
            
            return {
                'added': len(documents),
                'total_docs': self.collection.count()
            }
            
        except Exception as e:
            logger.error(f"Error adding documents: {e}")
            raise
    
    def query(
        self,
        query_text: str,
        n_results: int = 5,
        where: Optional[Dict] = None
    ) -> Dict:
        """
        Semantic search - find similar documents
        
        Args:
            query_text: Search query
            n_results: How many results to return
            where: Metadata filters (e.g., {"url": "example.com"})
            
        Returns:
            dict: Search results with documents and metadata
            
        How it works:
        1. Query text → Embedding
        2. Find closest embeddings in database (cosine similarity)
        3. Return most similar documents
        
        Example:
        Query: "inspirational quotes"
        Finds: Documents about motivation, success, life lessons
        Even if they don't contain exact words!
        """
        try:
            results = self.collection.query(
                query_texts=[query_text],
                n_results=n_results,
                where=where  # Optional metadata filter
            )
            
            logger.info(f"Query: '{query_text}' returned {len(results['documents'][0])} results")
            
            # Format results nicely
            formatted_results = {
                'query': query_text,
                'num_results': len(results['documents'][0]),
                'results': []
            }
            
            # Unpack results
            for i in range(len(results['documents'][0])):
                formatted_results['results'].append({
                    'id': results['ids'][0][i],
                    'document': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i],
                    'distance': results['distances'][0][i] if 'distances' in results else None
                })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error querying ChromaDB: {e}")
            raise
    
    def get_by_id(self, doc_id: str) -> Optional[Dict]:
        """
        Retrieve specific document by ID
        
        Args:
            doc_id: Document ID
            
        Returns:
            dict: Document with metadata
            
        Why needed?
        - Quick lookup without search
        - Verify specific document exists
        - Update/delete operations
        """
        try:
            result = self.collection.get(ids=[doc_id])
            
            if result['documents']:
                return {
                    'id': result['ids'][0],
                    'document': result['documents'][0],
                    'metadata': result['metadatas'][0]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting document {doc_id}: {e}")
            return None
    
    def delete_by_id(self, doc_id: str) -> bool:
        """
        Delete document from database
        
        Args:
            doc_id: Document ID
            
        Returns:
            bool: Success status
        """
        try:
            self.collection.delete(ids=[doc_id])
            logger.info(f"Deleted document: {doc_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting {doc_id}: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """
        Get database statistics
        
        Returns:
            dict: Stats about the collection
            
        Why useful?
        - Monitor database size
        - Check if data indexed
        - Debug issues
        """
        return {
            'collection_name': self.collection_name,
            'total_documents': self.collection.count(),
            'persist_directory': self.persist_directory
        }
    
    def reset_database(self):
        """
        Delete all data - use carefully!
        
        Why needed?
        - Testing
        - Fresh start
        - Clean up corrupted data
        """
        self.client.delete_collection(self.collection_name)
        self.collection = self.client.create_collection(self.collection_name)
        logger.warning(f"Reset collection: {self.collection_name}")


def create_embeddings_from_mongodb(
    mongo_uri: str = 'mongodb://localhost:27017',
    db_name: str = 'rag_scraper',
    chroma_dir: str = './data/chromadb'
) -> Dict:
    """
    Load processed data from MongoDB and create embeddings
    
    Args:
        mongo_uri: MongoDB connection
        db_name: Database name
        chroma_dir: ChromaDB directory
        
    Returns:
        dict: Statistics
        
    This is the bridge:
    MongoDB (processed text) → ChromaDB (searchable embeddings)
    """
    from pymongo import MongoClient
    
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    processed_collection = db['processed_data']
    
    # Initialize ChromaDB
    chroma = ChromaManager(persist_directory=chroma_dir)
    
    # Get documents from MongoDB
    docs = list(processed_collection.find({'ready_for_rag': True}))
    
    if not docs:
        logger.warning("No documents ready for RAG processing")
        return {'added': 0}
    
    logger.info(f"Found {len(docs)} documents ready for embedding")
    
    # Prepare data for ChromaDB
    documents = []
    metadatas = []
    ids = []
    
    for doc in docs:
        # Use clean_text for embedding
        # Why clean_text? Already processed, no HTML junk
        text = doc.get('clean_text', '')
        
        if not text or len(text) < 50:
            continue  # Skip too short
        
        documents.append(text)
        
        # Store metadata for context
        metadatas.append({
            'url': doc.get('url', ''),
            'title': doc.get('title', ''),
            'word_count': doc.get('word_count', 0),
            'scraped_at': doc.get('scraped_at', ''),
            'processed_at': doc.get('processed_at', '')
        })
        
        ids.append(str(doc['_id']))
    
    # Add to ChromaDB
    if documents:
        stats = chroma.add_documents(documents, metadatas, ids)
        logger.info(f"Created embeddings for {stats['added']} documents")
        return stats
    
    return {'added': 0}