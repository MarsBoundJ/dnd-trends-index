import google.generativeai as genai
import os

API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

def list_gemini_models():
    print(f"Listing models for Gemini Developer API...")
    try:
        genai.configure(api_key=API_KEY)
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                print(f"Model ID: {m.name}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    list_gemini_models()
