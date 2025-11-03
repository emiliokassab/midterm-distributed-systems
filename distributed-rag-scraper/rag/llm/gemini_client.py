"""
Google Gemini LLM Client for RAG - Production Ready
Handles safety filters and edge cases properly
"""
import google.generativeai as genai
import os
from typing import List, Dict, Optional
import logging
import time

logger = logging.getLogger(__name__)


class GeminiRAGClient:
    """
    Production-ready RAG client using Google Gemini 2.x
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "models/gemini-2.5-flash",
        chroma_manager = None
    ):
        """Initialize Gemini RAG client with safety handling"""
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        
        if not self.api_key:
            logger.warning("No Gemini API key. Set GEMINI_API_KEY environment variable.")
            self.model = None
        else:
            try:
                genai.configure(api_key=self.api_key)
                
                # Configure with relaxed safety settings
                safety_settings = [
                    {
                        "category": "HARM_CATEGORY_HARASSMENT",
                        "threshold": "BLOCK_ONLY_HIGH"
                    },
                    {
                        "category": "HARM_CATEGORY_HATE_SPEECH",
                        "threshold": "BLOCK_ONLY_HIGH"
                    },
                    {
                        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                        "threshold": "BLOCK_ONLY_HIGH"
                    },
                    {
                        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                        "threshold": "BLOCK_ONLY_HIGH"
                    }
                ]
                
                self.model = genai.GenerativeModel(
                    model_name,
                    safety_settings=safety_settings
                )
                logger.info(f"✅ Gemini RAG Client initialized with {model_name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize {model_name}: {e}")
                self.model = None
        
        self.model_name = model_name
        self.chroma = chroma_manager
        self.last_request_time = 0
        self.min_request_interval = 1.0
    
    def _rate_limit(self):
        """Rate limiting to avoid API limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
    
    def query(
        self,
        question: str,
        n_results: int = 3,
        temperature: float = 0.7
    ) -> Dict:
        """RAG query with improved error handling"""
        try:
            # Step 1: Retrieve documents
            logger.info(f"Searching for: {question}")
            search_results = self.chroma.query(
                query_text=question,
                n_results=n_results
            )
            
            if not search_results['results']:
                return {
                    'answer': 'No relevant information found in the knowledge base.',
                    'sources': [],
                    'question': question
                }
            
            # Step 2: Build context
            context = self._build_context(search_results['results'])
            
            # Step 3: Create prompt
            prompt = self._create_prompt(question, context)
            
            # Step 4: Get Gemini response
            if not self.model:
                return {
                    'answer': 'Gemini model not initialized.',
                    'sources': self._format_sources(search_results['results']),
                    'question': question
                }
            
            # Apply rate limiting
            self._rate_limit()
            
            # Generate with retry and safety handling
            answer = self._generate_with_safety(prompt, temperature)
            
            return {
                'answer': answer,
                'sources': self._format_sources(search_results['results']),
                'question': question,
                'model': self.model_name
            }
            
        except Exception as e:
            logger.error(f"Error in Gemini RAG query: {e}")
            return {
                'answer': f'Error generating response: {str(e)}',
                'sources': [],
                'question': question
            }
    
    def _generate_with_safety(self, prompt: str, temperature: float) -> str:
        """Generate response with safety filter handling"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                generation_config = {
                    "temperature": temperature,
                    "max_output_tokens": 1000,
                    "top_p": 0.95,
                    "top_k": 40,
                }
                
                response = self.model.generate_content(
                    prompt,
                    generation_config=generation_config
                )
                
                # Check if response was blocked
                if response.candidates:
                    candidate = response.candidates[0]
                    
                    # Check finish reason
                    if hasattr(candidate, 'finish_reason'):
                        finish_reason = candidate.finish_reason
                        
                        # Finish reasons: 1=STOP, 2=SAFETY, 3=MAX_TOKENS, 4=RECITATION
                        if finish_reason == 2:  # SAFETY
                            logger.warning("Response blocked by safety filter, trying with rephrased prompt")
                            # Rephrase prompt to be more neutral
                            prompt = self._rephrase_for_safety(prompt)
                            if attempt < max_retries - 1:
                                time.sleep(1)
                                continue
                            else:
                                return "The response was filtered for safety reasons. Please try rephrasing your question."
                        
                        elif finish_reason == 4:  # RECITATION
                            return "The model detected potential copyright content. Please try a different question."
                    
                    # Try to get text from response
                    if hasattr(candidate, 'content') and hasattr(candidate.content, 'parts'):
                        if candidate.content.parts:
                            return candidate.content.parts[0].text
                
                # Fallback: try direct text access
                if hasattr(response, 'text'):
                    return response.text
                    
            except AttributeError as e:
                # Handle the specific "response.text" error
                if "response.text" in str(e):
                    logger.warning(f"Attempt {attempt + 1}: Response has no text (likely filtered)")
                    if attempt < max_retries - 1:
                        # Try with a more neutral prompt
                        prompt = self._rephrase_for_safety(prompt)
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        return "Unable to generate response. The content may have been filtered. Please try rephrasing."
                else:
                    raise e
                    
            except Exception as api_error:
                error_msg = str(api_error)
                if "429" in error_msg or "quota" in error_msg.lower():
                    wait_time = 2 ** (attempt + 1)
                    logger.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                elif attempt < max_retries - 1:
                    logger.warning(f"API attempt {attempt + 1} failed: {api_error}")
                    time.sleep(2 ** attempt)
                else:
                    raise api_error
        
        return "Failed to generate response after multiple attempts."
    
    def _rephrase_for_safety(self, prompt: str) -> str:
        """Rephrase prompt to avoid safety filters"""
        # Add safety context to prompt
        safety_prefix = """Please provide a helpful, harmless, and honest response. 
Focus on educational and informative content.

"""
        return safety_prefix + prompt
    
    def _build_context(self, results: List[Dict]) -> str:
        """Build context from retrieved documents"""
        context_parts = []
        
        for i, result in enumerate(results, 1):
            doc_text = result['document']
            url = result['metadata'].get('url', 'Unknown')
            title = result['metadata'].get('title', 'Untitled')
            
            if len(doc_text) > 1000:
                doc_text = doc_text[:1000] + "..."
            
            context_parts.append(
                f"[Source {i}: {title}]\n"
                f"URL: {url}\n"
                f"Content: {doc_text}\n"
            )
        
        return "\n---\n".join(context_parts)
    
    def _create_prompt(self, question: str, context: str) -> str:
        """Create prompt for Gemini"""
        prompt = f"""You are a helpful AI assistant using a RAG system.
Based on the following context, provide an informative answer.

CONTEXT:
{context}

QUESTION: {question}

Please provide a clear, factual answer based on the context. Include source references [Source X] where appropriate."""
        
        return prompt
    
    def _format_sources(self, results: List[Dict]) -> List[Dict]:
        """Format sources for response"""
        sources = []
        
        for i, result in enumerate(results, 1):
            sources.append({
                'source_number': i,
                'url': result['metadata'].get('url', 'Unknown'),
                'title': result['metadata'].get('title', 'Untitled'),
                'relevance_score': 1 - result.get('distance', 0),
                'preview': result['document'][:200] + "..." if len(result['document']) > 200 else result['document']
            })
        
        return sources