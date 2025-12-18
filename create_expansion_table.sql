
CREATE TABLE `dnd-trends-index.dnd_trends_categorized.expanded_search_terms`
(
  term_id STRING OPTIONS(description="Unique UUID for this expansion instance"),
  original_keyword STRING OPTIONS(description="The source keyword from concept_library"),
  category STRING OPTIONS(description="Category from concept_library"),
  search_term STRING OPTIONS(description="The actual string to query in Google Trends"),
  expansion_rule STRING OPTIONS(description="The logic used to generate this term (e.g. 'suffix_5e', 'nickname_gen')"),
  created_at TIMESTAMP OPTIONS(description="When this term was generated"),
  is_pilot BOOL OPTIONS(description="Flag to easily identify pilot data")
);
