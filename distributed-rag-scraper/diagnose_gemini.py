"""
Diagnostic script for Gemini API - Updated for Gemini 2.x models
This will help identify available models and test your API key
"""
import os
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def diagnose_gemini():
    """Diagnose Gemini API setup"""
    
    print("=" * 60)
    print("GEMINI API DIAGNOSTIC - UPDATED")
    print("=" * 60)
    
    # Check API key
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        print("❌ No API key found in environment!")
        print("\nTo fix:")
        print("1. Get key from: https://aistudio.google.com/app/apikey")
        print("2. Add to .env: GEMINI_API_KEY=your-key-here")
        return
    
    print(f"✅ API key found (length: {len(api_key)} chars)")
    print("⚠️  Remember: Never share your API key publicly!")
    
    # Configure API
    try:
        genai.configure(api_key=api_key)
        print("✅ API configured successfully")
    except Exception as e:
        print(f"❌ Failed to configure API: {e}")
        return
    
    # Test the new Gemini 2.x models
    print("\n" + "=" * 60)
    print("TESTING GEMINI 2.x MODELS:")
    print("=" * 60)
    
    # Updated list based on your available models
    test_models = [
        "models/gemini-2.0-flash",           # Fast and free
        "models/gemini-2.0-flash-lite",      # Even lighter version
        "models/gemini-2.5-flash",           # Latest stable flash
        "models/gemini-2.5-flash-lite",      # Latest lite version
        "models/gemini-flash-latest",        # Latest flash alias
        "gemini-2.0-flash",                  # Try without models/ prefix
        "gemini-2.5-flash",                  # Try without models/ prefix
    ]
    
    working_models = []
    best_model = None
    
    for model_name in test_models:
        print(f"\nTesting: {model_name}")
        try:
            model = genai.GenerativeModel(model_name)
            response = model.generate_content("Say 'Hello World' in exactly 2 words")
            print(f"✅ {model_name} WORKS!")
            print(f"   Response: {response.text.strip()}")
            working_models.append(model_name)
            if not best_model and "2.5" in model_name and "flash" in model_name:
                best_model = model_name  # Prefer 2.5 flash as it's newer
            elif not best_model and "2.0" in model_name and "flash" in model_name:
                best_model = model_name  # Fallback to 2.0 flash
        except Exception as e:
            error_msg = str(e)
            if "404" in error_msg or "not found" in error_msg:
                print(f"❌ {model_name} not found")
            elif "429" in error_msg:
                print(f"⚠️  {model_name} rate limited (try again later)")
            elif "quota" in error_msg.lower():
                print(f"⚠️  {model_name} quota exceeded")
            else:
                print(f"❌ {model_name} error: {error_msg[:100]}")
    
    # If no best model selected yet, pick the first working one
    if not best_model and working_models:
        best_model = working_models[0]
    
    # Show summary of available models
    print("\n" + "=" * 60)
    print("AVAILABLE MODELS SUMMARY:")
    print("=" * 60)
    
    print("\n📋 Models you have access to:")
    print("  • Gemini 2.5 Flash (Latest, best performance)")
    print("  • Gemini 2.5 Flash-Lite (Faster, lighter)")
    print("  • Gemini 2.0 Flash (Stable, reliable)")
    print("  • Gemini 2.0 Flash-Lite (Fastest response)")
    
    # Recommendations
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS:")
    print("=" * 60)
    
    if best_model:
        print(f"\n✅ RECOMMENDED MODEL: '{best_model}'")
        print(f"\n📝 Update your gemini_client.py file:")
        print(f"\n   Change this line:")
        print(f'   model_name: str = "gemini-1.5-flash"')
        print(f"\n   To this:")
        print(f'   model_name: str = "{best_model}"')
        
        print(f"\n🎯 All working models found:")
        for m in working_models:
            print(f"   • {m}")
            
        print("\n💡 Model Selection Guide:")
        print("   • Use gemini-2.5-flash for best quality")
        print("   • Use gemini-2.5-flash-lite for faster responses")
        print("   • Use gemini-2.0-flash for stability")
        
    else:
        print("\n❌ No working models found!")
        print("\nTroubleshooting:")
        print("1. Check if your API key has proper permissions")
        print("2. Try regenerating your API key")
        print("3. Check for regional restrictions")
        print("4. Verify quota limits haven't been exceeded")
    
    print("\n" + "=" * 60)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    diagnose_gemini()