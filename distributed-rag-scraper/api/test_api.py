"""
Test FastAPI Service
Verifies all API endpoints are working correctly
"""
import requests
import json
import logging
from typing import Dict, Any
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APITester:
    def __init__(self, base_url: str = "http://localhost:8000", token: str = "demo-token-123"):
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        self.results = []
    
    def test_endpoint(self, method: str, endpoint: str, data: Dict[str, Any] = None, name: str = None):
        """Test a single endpoint"""
        url = f"{self.base_url}{endpoint}"
        name = name or endpoint
        
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers, params=data)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            success = response.status_code in [200, 201]
            
            result = {
                "name": name,
                "method": method,
                "endpoint": endpoint,
                "status": response.status_code,
                "success": success,
                "response": response.json() if success else response.text
            }
            
            self.results.append(result)
            
            if success:
                logger.info(f"✅ {name}: {response.status_code}")
            else:
                logger.error(f"❌ {name}: {response.status_code} - {response.text}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ {name}: {e}")
            self.results.append({
                "name": name,
                "method": method,
                "endpoint": endpoint,
                "status": 0,
                "success": False,
                "error": str(e)
            })
            return None
    
    def run_all_tests(self):
        """Run all API tests"""
        logger.info("=" * 60)
        logger.info("TESTING FASTAPI SERVICE")
        logger.info("=" * 60)
        
        # Test root endpoint
        self.test_endpoint("GET", "/", name="Root Endpoint")
        
        # Test health check
        self.test_endpoint("GET", "/health", name="Health Check")
        
        # Test raw data retrieval
        self.test_endpoint(
            "GET", 
            "/api/v1/raw-data",
            {"limit": 5, "skip": 0},
            name="Get Raw Data"
        )
        
        # Test processed data retrieval
        self.test_endpoint(
            "GET",
            "/api/v1/processed-data",
            {"limit": 5, "skip": 0},
            name="Get Processed Data"
        )
        
        # Test search
        self.test_endpoint(
            "POST",
            "/api/v1/search",
            {
                "query": "inspirational quotes",
                "n_results": 3,
                "use_rag": False
            },
            name="Search Data"
        )
        
        # Test RAG query
        self.test_endpoint(
            "POST",
            "/api/v1/rag/query",
            {
                "query": "What are the most popular quotes about life?",
                "n_results": 3,
                "use_rag": True
            },
            name="RAG Query"
        )
        
        # Test statistics
        self.test_endpoint("GET", "/api/v1/stats", name="Get Statistics")
        
        # Test scrape submission
        self.test_endpoint(
            "POST",
            "/api/v1/scrape",
            {
                "url": "https://example.com",
                "depth": 1
            },
            name="Submit Scrape Job"
        )
        
        # Summary
        logger.info("=" * 60)
        logger.info("TEST RESULTS SUMMARY")
        logger.info("=" * 60)
        
        success_count = sum(1 for r in self.results if r["success"])
        total_count = len(self.results)
        
        logger.info(f"Total Tests: {total_count}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {total_count - success_count}")
        logger.info(f"Success Rate: {success_count/total_count*100:.1f}%")
        
        if success_count == total_count:
            logger.info("🎉 All tests passed!")
        else:
            logger.warning("⚠️ Some tests failed. Check logs above.")
        
        return self.results

def main():
    """Main test function"""
    
    # Check if API is running
    try:
        response = requests.get("http://localhost:8000/")
        logger.info("✅ API is running")
    except:
        logger.error("❌ API is not running. Start it with: python api_service.py")
        return
    
    # Run tests
    tester = APITester()
    results = tester.run_all_tests()
    
    # Save results
    with open("api_test_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info("\nTest results saved to api_test_results.json")

if __name__ == "__main__":
    main()