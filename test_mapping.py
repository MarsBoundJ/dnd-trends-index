from google.cloud import bigquery
client = bigquery.Client('dnd-trends-index')
sql = """
WITH classes AS (
    SELECT DISTINCT concept_name as class_name
    FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
    WHERE category = 'Class'
),
subclass_tags AS (
    SELECT 
        concept_name as subclass_name,
        tag
    FROM `dnd-trends-index.dnd_trends_categorized.concept_library`,
    UNNEST(tags) as tag
    WHERE category LIKE '%Subclass%'
)
SELECT 
    s.subclass_name,
    c.class_name
FROM subclass_tags s
JOIN classes c ON s.tag = c.class_name OR s.tag = CONCAT(c.class_name, '_Subclasses')
GROUP BY 1, 2
"""
results = client.query(sql).result()
count = 0
for row in results:
    print(f"{row.subclass_name} -> {row.class_name}")
    count += 1
print(f"Total mapped: {count}")
