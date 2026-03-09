³import ahocorasick
import pickle
import os
from google.cloud import bigquery

# Config
KEYWORD_CACHE_FILE = 'keyword_trie.pkl'
DATASET_ID = 'dnd-trends-index.dnd_trends_categorized'
TABLE_ID = f'{DATASET_ID}.expanded_search_terms'

class KeywordMatcher:
    def __init__(self, refresh=False):
        self.automaton = ahocorasick.Automaton()
        self.client = bigquery.Client()
        self.keyword_map = {} # ID -> Term
        
        if os.path.exists(KEYWORD_CACHE_FILE) and not refresh:
            print("Loading keyword trie from cache...")
            self.load_cache()
        else:
            print("Fetching keywords from BigQuery...")
            self.build_trie()

    def build_trie(self):
        query = f"""
            SELECT search_term, category, original_keyword 
            FROM `{TABLE_ID}`
        """
        rows = self.client.query(query).result()
        
        count = 0
        for row in rows:
            term = row.search_term.lower()
            # Store metadata we might need later (Category, Original)
            payload = {
                "term": row.search_term,
                "category": row.category,
                "original": row.original_keyword
            }
            # Add to automaton
            # Note: Aho-Corasick matches patterns. 
            self.automaton.add_word(term, payload)
            count += 1
            
        print(f"Building automaton with {count} terms...")
        self.automaton.make_automaton()
        print("Automaton built.")
        
        # Cache it
        with open(KEYWORD_CACHE_FILE, 'wb') as f:
            pickle.dump(self.automaton, f)

    def load_cache(self):
        with open(KEYWORD_CACHE_FILE, 'rb') as f:
            self.automaton = pickle.load(f)

    def find_matches(self, text):
        """
        Returns a list of unique matched payloads found in the text.
        Text is normalized to lowercase.
        """
        text_lower = text.lower()
        matches = []
        seen = set()
        
        # iter() returns (end_index, value)
        for end_index, payload in self.automaton.iter(text_lower):
            term = payload['term']
            if term not in seen:
                matches.append(payload)
                seen.add(term)
                
        return matches

if __name__ == "__main__":
    # Test Run
    matcher = KeywordMatcher(refresh=True)
    
    test_text = "I am building a Hexblade Warlock with a Holy Avenger."
    print(f"\nTest Text: '{test_text}'")
    hits = matcher.find_matches(test_text)
    print(f"Hits: {len(hits)}")
    for h in hits:
        print(f" - {h['term']} ({h['category']})")
³"(0346c7b262db785b9f82b154e34994382565350e2)file:///C:/Users/Yorri/.gemini/matcher.py:file:///C:/Users/Yorri/.gemini