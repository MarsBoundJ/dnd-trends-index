import google.generativeai as genai
import os

API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

def test_gemini_api():
    print(f"Testing Gemini Developer API with key ending in ...{API_KEY[-4:]}")
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content("Hi")
        print(f"SUCCESS: Generated content with Gemini Developer API: {response.text}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    test_gemini_api()
