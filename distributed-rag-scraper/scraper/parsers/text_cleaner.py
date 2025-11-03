"""
Text Cleaning and Normalization Module
"""
import re
from typing import List


class TextCleaner:
    """
    Clean and normalize extracted text
    """
    
    def __init__(self):
        """Initialize text cleaner"""
        pass
    
    def clean(self, text: str) -> str:
        """
        Clean text by removing unwanted characters and normalizing
        
        Args:
            text: Raw text
            
        Returns:
            str: Cleaned text
        """
        if not text:
            return ''
        
        # Remove extra whitespace
        text = self._normalize_whitespace(text)
        
        # Remove special characters (keep basic punctuation)
        text = self._remove_special_chars(text)
        
        # Fix common issues
        text = self._fix_common_issues(text)
        
        return text.strip()
    
    def _normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace"""
        # Replace multiple spaces with single space
        text = re.sub(r' +', ' ', text)
        
        # Replace multiple newlines with double newline
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Remove trailing whitespace from lines
        lines = [line.rstrip() for line in text.split('\n')]
        text = '\n'.join(lines)
        
        return text
    
    def _remove_special_chars(self, text: str) -> str:
        """Remove unwanted special characters"""
        # Keep alphanumeric, basic punctuation, and whitespace
        text = re.sub(r'[^\w\s.,!?;:\'"()\-–—]', '', text)
        return text
    
    def _fix_common_issues(self, text: str) -> str:
        """Fix common text issues"""
        # Fix multiple periods
        text = re.sub(r'\.{4,}', '...', text)
        
        # Fix spaces before punctuation
        text = re.sub(r'\s+([.,!?;:])', r'\1', text)
        
        # Fix spaces after opening brackets
        text = re.sub(r'([\(\[])\s+', r'\1', text)
        
        # Fix spaces before closing brackets
        text = re.sub(r'\s+([\)\]])', r'\1', text)
        
        return text
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text into words
        
        Args:
            text: Input text
            
        Returns:
            list: List of tokens
        """
        # Simple word tokenization
        words = re.findall(r'\b\w+\b', text.lower())
        return words
    
    def remove_stopwords(self, tokens: List[str]) -> List[str]:
        """
        Remove common stopwords
        
        Args:
            tokens: List of tokens
            
        Returns:
            list: Filtered tokens
        """
        # Basic English stopwords
        stopwords = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for',
            'from', 'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on',
            'that', 'the', 'to', 'was', 'will', 'with', 'this', 'but'
        }
        
        return [token for token in tokens if token not in stopwords]


def clean_text(text: str) -> str:
    """
    Convenience function to clean text
    
    Args:
        text: Raw text
        
    Returns:
        str: Cleaned text
    """
    cleaner = TextCleaner()
    return cleaner.clean(text)