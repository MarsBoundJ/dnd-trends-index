from google.cloud import bigquery

client = bigquery.Client(project="dnd-trends-index")
query = "SELECT Keyword, Category FROM `dnd-trends-index.dnd_keywords.dnd_keywords` WHERE Category = 'Monster' ORDER BY Keyword LIMIT 20"
rows = client.query(query).result()

print("+----------------------------+---------+")
print("| Keyword                    | Category|")
print("+----------------------------+---------+")
for row in rows:
    print(f"| {row.Keyword:<26} | {row.Category:<7} |")
print("+----------------------------+---------+")
