import ahocorasick
import pickle
import os
import re
from google.cloud import bigquery
from nltk.stem import SnowballStemmer

# Config
KEYWORD_CACHE_FILE = 'keyword_trie.pkl'
DATASET_ID = 'dnd-trends-index.dnd_trends_categorized'
TABLE_ID = f'{DATASET_ID}.expanded_search_terms'

class KeywordMatcher:
    def __init__(self, refresh=False):
        self.automaton = ahocorasick.Automaton()
        self.client = bigquery.Client()
        self.stemmer = SnowballStemmer("english")
        
        if os.path.exists(KEYWORD_CACHE_FILE) and not refresh:
            print("Loading keyword trie from cache...")
            self.load_cache()
        else:
            print("Fetching keywords from BigQuery...")
            self.build_trie()

    def stem_text(self, text):
        """
        Tokenizes text, stems each word, and pads with spaces for boundary matching.
        """
        if not text:
            return ""
        # Simple tokenization: split by non-alphanumeric
        tokens = re.findall(r'\b\w+\b', text.lower())
        stemmed_tokens = [self.stemmer.stem(t) for t in tokens]
        # Pad with spaces to ensure word boundary matching in Aho-Corasick
        return " " + " ".join(stemmed_tokens) + " "

    def build_trie(self):
        # 1. Fetch base concepts
        query_base = f"SELECT concept_name as term, category FROM `{DATASET_ID}.concept_library`"
        # 2. Fetch expanded concepts
        query_expanded = f"SELECT search_term as term, category FROM `{TABLE_ID}`"
        
        count = 0
        self.automaton = ahocorasick.Automaton() # Reset
        
        for query in [query_base, query_expanded]:
            rows = self.client.query(query).result()
            for row in rows:
                term_raw = row.term
                term_stemmed = self.stem_text(term_raw)
                # Ensure we don't add empty padded strings
                if not term_stemmed or term_stemmed.strip() == "":
                    continue
                    
                payload = {
                    "term": term_raw,
                    "category": row.category
                }
                
                try:
                    self.automaton.add_word(term_stemmed, payload)
                    count += 1
                except Exception:
                    pass
            
        print(f"Building automaton with {count} stemmed terms from both libraries...")
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
        Stems input text and returns unique matched payloads.
        """
        if not text:
            return []
            
        text_stemmed = self.stem_text(text)
        matches = []
        seen = set()
        
        for end_index, payload in self.automaton.iter(text_stemmed):
            term = payload['term']
            if term not in seen:
                matches.append(payload)
                seen.add(term)
                
        return matches

if __name__ == "__main__":
    # Test Run
    matcher = KeywordMatcher(refresh=True)
    
    test_cases = [
        "I am building a Hexblade Warlock with a Holy Avenger.",
        "Check out these fighters and their martial maneuvers.",
        "A band of goblins attacks the clerics."
    ]
    
    for test_text in test_cases:
        print(f"\nTarget: '{test_text}'")
        hits = matcher.find_matches(test_text)
        print(f"Hits: {len(hits)}")
        for h in hits:
            print(f" - {h['term']} ({h['category']})")
