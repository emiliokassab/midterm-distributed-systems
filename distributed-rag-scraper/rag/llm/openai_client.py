"""
OpenAI LLM Client for RAG
Integrates GPT models with vector database for intelligent responses

Why OpenAI?
- State-of-the-art language understanding
- Powerful summarization capabilities
- Easy API integration
- Reliable and fast
"""
from openai import OpenAI
import os
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class RAGClient:
    """
    RAG client combining ChromaDB retrieval with OpenAI generation
    
    How it works:
    1. User asks question
    2. Find relevant docs (ChromaDB)
    3. Create context from docs
    4. Send to GPT with prompt
    5. Return intelligent answer
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-3.5-turbo",
        chroma_manager = None
    ):
        """
        Initialize RAG client
        
        Args:
            api_key: OpenAI API key (or from env)
            model: GPT model to use
            chroma_manager: ChromaDB manager instance
            
        Why gpt-3.5-turbo?
        - Fast responses
        - Cost-effective
        - Good quality
        - Can upgrade to gpt-4 later
        """
        # Get API key from environment or parameter
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        
        if not self.api_key:
            logger.warning("No OpenAI API key provided. Set OPENAI_API_KEY environment variable.")
        
        self.model = model
        self.client = OpenAI(api_key=self.api_key) if self.api_key else None
        self.chroma = chroma_manager
        
        logger.info(f"RAG Client initialized with model: {model}")
    
    def query(
        self,
        question: str,
        n_results: int = 3,
        temperature: float = 0.7
    ) -> Dict:
        """
        RAG query: Retrieve relevant docs + Generate answer
        
        Args:
            question: User's question
            n_results: How many docs to retrieve
            temperature: LLM creativity (0=factual, 1=creative)
            
        Returns:
            dict: Answer with sources
            
        Why temperature 0.7?
        - Balance between accuracy and natural language
        - Not too robotic (0.0)
        - Not too creative (1.0)
        """
        try:
            # Step 1: Retrieve relevant documents (semantic search)
            logger.info(f"Searching for: {question}")
            search_results = self.chroma.query(
                query_text=question,
                n_results=n_results
            )
            
            if not search_results['results']:
                return {
                    'answer': 'No relevant information found in the database.',
                    'sources': [],
                    'question': question
                }
            
            # Step 2: Build context from retrieved documents
            context = self._build_context(search_results['results'])
            
            # Step 3: Create prompt for LLM
            prompt = self._create_prompt(question, context)
            
            # Step 4: Get LLM response
            if not self.client:
                # Fallback if no API key
                return {
                    'answer': 'OpenAI API key not configured. Retrieved documents shown below.',
                    'sources': self._format_sources(search_results['results']),
                    'question': question,
                    'context': context
                }
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant that answers questions based on provided context. Always cite sources and be factual."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=temperature,
                max_tokens=500
            )
            
            answer = response.choices[0].message.content
            
            # Step 5: Return formatted response
            return {
                'answer': answer,
                'sources': self._format_sources(search_results['results']),
                'question': question,
                'model': self.model,
                'tokens_used': response.usage.total_tokens if hasattr(response, 'usage') else None
            }
            
        except Exception as e:
            logger.error(f"Error in RAG query: {e}")
            return {
                'answer': f'Error processing query: {str(e)}',
                'sources': [],
                'question': question
            }
    
    def _build_context(self, results: List[Dict]) -> str:
        """
        Build context string from retrieved documents
        
        Args:
            results: Search results from ChromaDB
            
        Returns:
            str: Formatted context
            
        Why format context?
        - LLM needs clean, organized input
        - Include source information
        - Stay within token limits
        """
        context_parts = []
        
        for i, result in enumerate(results, 1):
            doc_text = result['document']
            url = result['metadata'].get('url', 'Unknown')
            title = result['metadata'].get('title', 'Untitled')
            
            # Truncate long documents
            if len(doc_text) > 500:
                doc_text = doc_text[:500] + "..."
            
            context_parts.append(
                f"[Source {i}: {title}]\n"
                f"URL: {url}\n"
                f"Content: {doc_text}\n"
            )
        
        return "\n---\n".join(context_parts)
    
    def _create_prompt(self, question: str, context: str) -> str:
        """
        Create prompt for LLM
        
        Args:
            question: User's question
            context: Retrieved context
            
        Returns:
            str: Formatted prompt
            
        Why careful prompting?
        - Guides LLM behavior
        - Ensures factual responses
        - Encourages source citation
        """
        prompt = f"""Based on the following context from scraped web content, please answer the question.
Be factual and cite the sources. If the context doesn't contain relevant information, say so.

CONTEXT:
{context}

QUESTION: {question}

Please provide a clear, concise answer based on the context above. Mention which sources you used."""
        
        return prompt
    
    def _format_sources(self, results: List[Dict]) -> List[Dict]:
        """
        Format sources for response
        
        Args:
            results: Search results
            
        Returns:
            list: Formatted source information
        """
        sources = []
        
        for i, result in enumerate(results, 1):
            sources.append({
                'source_number': i,
                'url': result['metadata'].get('url', 'Unknown'),
                'title': result['metadata'].get('title', 'Untitled'),
                'relevance_score': 1 - result.get('distance', 0),
                'preview': result['document'][:200] + "..."
            })
        
        return sources
    
    def summarize_document(self, doc_id: str) -> Dict:
        """
        Summarize a specific document
        
        Args:
            doc_id: Document ID from ChromaDB
            
        Returns:
            dict: Summary
            
        Why document summaries?
        - Quick overview of content
        - Useful for large documents
        - Can be cached
        """
        try:
            # Get document from ChromaDB
            doc = self.chroma.get_by_id(doc_id)
            
            if not doc:
                return {'error': 'Document not found'}
            
            if not self.client:
                return {'error': 'OpenAI API key not configured'}
            
            # Create summary prompt
            prompt = f"""Please provide a concise summary of the following content:

{doc['document']}

Summary:"""
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that creates concise summaries."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_tokens=200
            )
            
            return {
                'document_id': doc_id,
                'url': doc['metadata'].get('url'),
                'title': doc['metadata'].get('title'),
                'summary': response.choices[0].message.content
            }
            
        except Exception as e:
            logger.error(f"Error summarizing document: {e}")
            return {'error': str(e)}


class SimpleRAG:
    """
    Simplified RAG without OpenAI (for testing without API key)
    
    Why?
    - Test system without API costs
    - Demonstrate retrieval part
    - Useful for development
    """
    
    def __init__(self, chroma_manager):
        self.chroma = chroma_manager
        logger.info("Simple RAG initialized (no LLM)")
    
    def query(self, question: str, n_results: int = 3) -> Dict:
        """
        Simple query - retrieval only, no generation
        """
        results = self.chroma.query(question, n_results)
        
        return {
            'question': question,
            'retrieved_documents': results['results'],
            'note': 'This is retrieval-only. Add OpenAI API key for AI-generated answers.'
        }