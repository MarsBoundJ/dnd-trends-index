ê$import os
import ahocorasick
from google.cloud import bigquery
from matcher import KeywordMatcher # Reusing our Phase 4 matcher logic

# Config
DATASET_ID = "dnd-trends-index.commercial_data"
SOURCE_TABLE = f"{DATASET_ID}.kickstarter_projects"
DEST_TABLE = f"{DATASET_ID}.keyword_commercial_metrics"
CONCEPT_TABLE = "dnd-trends-index.dnd_trends_categorized.concept_library"

class CommercialJoiner:
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.automaton = ahocorasick.Automaton()
        self.keyword_map = {} # term -> {mentions: 0, pledged: 0.0}

    def load_keywords(self):
        print("Loading keywords from concept_library...")
        query = f"SELECT concept_name FROM `{CONCEPT_TABLE}`"
        rows = self.bq_client.query(query)
        
        count = 0
        for row in rows:
            term = row.concept_name.lower().strip()
            if term and len(term) > 2: # Skip tiny noise
                self.automaton.add_word(term, term)
                self.keyword_map[term] = {"mentions": 0, "pledged": 0.0}
                count += 1
        
        self.automaton.make_automaton()
        print(f"Loaded {count} keywords into Aho-Corasick Trie.")

    def process_projects(self):
        print("Loading Kickstarter Projects...")
        query = f"""
            SELECT project_id, name, blurb, pledged_usd 
            FROM `{SOURCE_TABLE}` 
            WHERE is_dnd_centric = TRUE AND status IN ('successful', 'live')
        """
        rows = self.bq_client.query(query)
        
        proj_count = 0
        for row in rows:
            text = (str(row.name) + " " + str(row.blurb)).lower()
            pledged = row.pledged_usd or 0.0
            
            # Find matches
            found_terms = set()
            for end_index, original_value in self.automaton.iter(text):
                found_terms.add(original_value)
            
            # Update metrics
            for term in found_terms:
                self.keyword_map[term]["mentions"] += 1
                self.keyword_map[term]["pledged"] += pledged
            
            proj_count += 1
            
        print(f"Processed {proj_count} projects against keyword library.")

    def save_results(self):
        print("Saving Commercial Metrics to BigQuery...")
        
        # Prepare Schema
        schema = [
            bigquery.SchemaField("concept_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ks_mentions", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("total_pledged_exposure", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()")
        ]
        
        # Create Table if needed
        table = bigquery.Table(DEST_TABLE, schema=schema)
        try:
            self.bq_client.create_table(table)
            print("Created destination table.")
        except Exception:
            pass # Exists
            
        # Prepare Rows
        rows_to_insert = []
        for term, data in self.keyword_map.items():
            if data["mentions"] > 0:
                rows_to_insert.append({
                    "concept_name": term,
                    "ks_mentions": data["mentions"],
                    "total_pledged_exposure": data["pledged"],
                    # last_updated handled by default
                })
        
        # Batch Insert (Truncate/Replace logic ideally, but simplest is append for now or truncate)
        # We will truncate first to avoid dupes
        job_config = bigquery.QueryJobConfig(write_disposition="WRITE_TRUNCATE")
        # Actually easier to use load_table_from_json for large sets
        
        if rows_to_insert:
            print(f"Uploading {len(rows_to_insert)} matched keywords...")
            # Using insert_rows_json for simplicity, but careful of limits. 
            # 18k rows is fine for streaming, but slow.
            # We'll chunk it.
            chunk_size = 500
            for i in range(0, len(rows_to_insert), chunk_size):
                chunk = rows_to_insert[i:i+chunk_size]
                errors = self.bq_client.insert_rows_json(DEST_TABLE, chunk)
                if errors: print(f"Errors: {errors}")
            print("Upload Complete.")
        else:
            print("No matches found.")

if __name__ == "__main__":
    joiner = CommercialJoiner()
    joiner.load_keywords()
    joiner.process_projects()
    joiner.save_results()
ê$"(0f1ceee2742f32be6a66898aa01f4fd3b072102f2Bfile:///C:/Users/Yorri/.gemini/kickstarter_commercial_weighting.py:file:///C:/Users/Yorri/.gemini